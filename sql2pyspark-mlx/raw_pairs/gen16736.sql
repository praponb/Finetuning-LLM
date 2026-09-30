SELECT id, cost, carrier, status FROM shipments WHERE order_id IS NULL OR MONTH(delivered_date) IN (5, 9, 10) ORDER BY id DESC LIMIT 10
