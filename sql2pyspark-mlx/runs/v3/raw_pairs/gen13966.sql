SELECT id, carrier, delivered_date, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY delivered_date DESC, id) AS delivered_date_row FROM shipments ORDER BY delivered_date DESC
