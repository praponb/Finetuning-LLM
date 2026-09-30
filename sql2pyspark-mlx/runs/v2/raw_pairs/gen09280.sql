SELECT status, LENGTH(carrier) AS carrier_length FROM shipments WHERE shipped_date >= '2024-10-16' OR cost > 34;
