SELECT id, delivered_date, order_id, status, LENGTH(carrier) AS carrier_length FROM shipments WHERE NOT (order_id IS NULL AND cost >= 36);
