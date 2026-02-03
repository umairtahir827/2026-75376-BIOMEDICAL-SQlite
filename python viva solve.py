import sqlite3
from pprint import pprint

DB_NAME = "biomed_study.db"


def connect_db():
    conn = sqlite3.connect(DB_NAME)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def create_tables(conn):
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS Patients (
            patient_id INTEGER PRIMARY KEY AUTOINCREMENT,
            full_name TEXT NOT NULL,
            age INTEGER CHECK(age >= 18 AND age <= 90),
            gender TEXT CHECK(gender IN ('Male', 'Female', 'Other')),
            enrollment_date TEXT NOT NULL
        );
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS Clinical_Visits (
            visit_id INTEGER PRIMARY KEY AUTOINCREMENT,
            patient_id INTEGER NOT NULL,
            visit_date TEXT NOT NULL,
            systolic_bp INTEGER,
            diastolic_bp INTEGER,
            blood_glucose_mmol_l REAL,
            notes TEXT,
            FOREIGN KEY (patient_id)
                REFERENCES Patients(patient_id)
                ON DELETE CASCADE
        );
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS Samples (
            sample_id INTEGER PRIMARY KEY AUTOINCREMENT,
            patient_id INTEGER NOT NULL,
            collection_date TEXT NOT NULL,
            sample_type TEXT CHECK(sample_type IN ('Blood', 'Serum', 'Plasma', 'Urine')),
            storage_location TEXT,
            FOREIGN KEY (patient_id)
                REFERENCES Patients(patient_id)
                ON DELETE CASCADE
        );
        """
    )

    conn.commit()


def insert_sample_data(conn):
    cursor = conn.cursor()

    # Clear existing data to avoid FK conflicts on reruns
    cursor.execute("DELETE FROM Samples;")
    cursor.execute("DELETE FROM Clinical_Visits;")
    cursor.execute("DELETE FROM Patients;")
    conn.commit()

    # Insert patients and capture their IDs dynamically
    patients = [
        ("Alice Brown", 45, "Female", "2025-01-10"),
        ("John Smith", 60, "Male", "2025-01-15"),
        ("Ravi Kumar", 35, "Other", "2025-02-01"),
    ]

    cursor.executemany(
        """
        INSERT INTO Patients (full_name, age, gender, enrollment_date)
        VALUES (?, ?, ?, ?);
        """,
        patients,
    )

    cursor.execute("SELECT patient_id FROM Patients ORDER BY patient_id;")
    patient_ids = [row[0] for row in cursor.fetchall()]

    visits = [
        (patient_ids[0], "2025-02-10", 150, 95, 8.2, "High BP observed"),
        (patient_ids[0], "2025-03-10", 138, 88, 7.9, "Improving"),
        (patient_ids[1], "2025-02-20", 160, 100, 9.1, "Medication adjusted"),
        (patient_ids[2], "2025-02-25", 120, 80, 6.5, "Normal"),
    ]

    cursor.executemany(
        """
        INSERT INTO Clinical_Visits
        (patient_id, visit_date, systolic_bp, diastolic_bp, blood_glucose_mmol_l, notes)
        VALUES (?, ?, ?, ?, ?, ?);
        """,
        visits,
    )

    samples = [
        (patient_ids[0], "2025-02-10", "Blood", "Fridge A-03"),
        (patient_ids[0], "2025-03-10", "Serum", "Fridge B-01"),
        (patient_ids[1], "2025-02-20", "Plasma", "Biobank Rack 5"),
        (patient_ids[2], "2025-02-25", "Urine", "Fridge C-02"),
        (patient_ids[1], "2025-03-01", "Blood", "Biobank Rack 2"),
    ]

    cursor.executemany(
        """
        INSERT INTO Samples (patient_id, collection_date, sample_type, storage_location)
        VALUES (?, ?, ?, ?);
        """,
        samples,
    )

    conn.commit()


def read_queries(conn):
    cursor = conn.cursor()

    print("\nAll patients (age and enrollment date):")
    cursor.execute(
        "SELECT full_name, age, enrollment_date FROM Patients;"
    )
    pprint(cursor.fetchall())

    print("\nVisits for patient_id = 1 (JOIN query):")
    cursor.execute(
        """
        SELECT p.full_name, v.visit_date, v.systolic_bp, v.diastolic_bp
        FROM Patients p
        JOIN Clinical_Visits v ON p.patient_id = v.patient_id
        WHERE p.patient_id = ?;
        """,
        (1,),
    )
    pprint(cursor.fetchall())

    print("\nPatients with systolic BP > 140:")
    cursor.execute(
        """
        SELECT DISTINCT p.full_name
        FROM Patients p
        JOIN Clinical_Visits v ON p.patient_id = v.patient_id
        WHERE v.systolic_bp > ?;
        """,
        (140,),
    )
    pprint(cursor.fetchall())


def update_sample(conn):
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE Samples
        SET storage_location = ?
        WHERE sample_id = ?;
        """,
        ("Biobank Rack 9", 1),
    )
    conn.commit()


def delete_patient(conn):
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM Patients WHERE patient_id = ?;",
        (3,),
    )
    conn.commit()


def main():
    conn = connect_db()
    create_tables(conn)
    insert_sample_data(conn)
    read_queries(conn)
    update_sample(conn)
    delete_patient(conn)
    conn.close()


if __name__ == "__main__":
    main()