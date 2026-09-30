SELECT MAX(delivered_date) AS last_delivered_date, MAX(cost) AS max_cost, COUNT(cost) AS cost_count
FROM shipments
WHERE shipped_date <= '2025-05-01' AND order_id = 22
