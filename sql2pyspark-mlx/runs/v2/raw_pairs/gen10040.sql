SELECT id, order_id, delivered_date, cost FROM shipments WHERE YEAR(shipped_date) >= 2025 AND order_id IS NULL AND MONTH(shipped_date) IN (6, 8, 9) ORDER BY id
