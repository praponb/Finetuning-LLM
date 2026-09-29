SELECT id, shipped_date, carrier, status, DATE_FORMAT(shipped_date, 'MMM yyyy') AS shipped_date_label FROM shipments WHERE carrier = 'Kerry' ORDER BY shipped_date_label DESC, id ASC LIMIT 3;
