
# Verbindung zur PostgreSQL-Datenbank
pg_conn = psycopg2.connect(
    dbname="dvdrental",
    user="postgres",
    password="1234",
    host="postgresdb",
    port=5432
)

pg_cursor = pg_conn.cursor()

# Verbindung zur RavenDB
store = document_store.DocumentStore(urls=["http://ravendb:8080"], database="your_ravendb_database")
store.initialize()

session = store.open_session()

try:
    # PostgreSQL-Abfrage
    pg_cursor.execute("SELECT * FROM your_table")

    # Daten von PostgreSQL abrufen und in RavenDB einfügen
    for row in pg_cursor.fetchall():
        document = {
            "id": f"your_unique_id_{row[0]}",  # Annahme: row[0] ist ein eindeutiger Bezeichner
            "field1": row[1],
            "field2": row[2],
            # Andere Felder entsprechend zuordnen
        }

        # In RavenDB speichern
        session.store(document)

    # Änderungen in RavenDB speichern
    session.save_changes()

finally:
    # Verbindungen schließen
    pg_cursor.close()
    pg_conn.close()
    session.dispose()
    store.dispose()
