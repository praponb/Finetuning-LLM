SELECT order_id, carrier FROM shipments WHERE order_id = 24 UNION ALL SELECT order_id, carrier FROM shipments WHERE delivered_date >= '2023-05-01' ORDER BY order_id;
