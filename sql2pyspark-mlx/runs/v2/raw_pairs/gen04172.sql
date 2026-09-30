SELECT id, carrier, status, shipped_date, ROW_NUMBER() OVER (PARTITION BY carrier, status ORDER BY shipped_date DESC, id) AS row_num FROM shipments WHERE cost <= 88 ORDER BY shipped_date DESC
