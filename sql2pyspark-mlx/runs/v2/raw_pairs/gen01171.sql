SELECT delivered_date FROM shipments WHERE status IN ('RETURNED', 'LOST', 'DELIVERED') UNION ALL SELECT delivered_date FROM shipments WHERE order_id = 17 ORDER BY delivered_date;
