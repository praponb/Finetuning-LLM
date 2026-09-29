SELECT id, status, carrier, order_id, LOWER(status) AS status_lower FROM shipments WHERE NOT (MONTH(shipped_date) IN (2, 3, 10) AND shipped_date BETWEEN '2025-07-01' AND '2025-09-01')
