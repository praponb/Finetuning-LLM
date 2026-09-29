SELECT id, order_id FROM shipments WHERE carrier NOT IN ('Kerry', 'Flash') AND delivered_date < '2024-10-01' AND cost < 80
