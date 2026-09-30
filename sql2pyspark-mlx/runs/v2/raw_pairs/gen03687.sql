SELECT id, delivered_date, shipped_date, ROUND(cost * 100, 2) AS cost_scaled FROM shipments WHERE shipped_date >= '2025-09-01' AND order_id IS NULL
