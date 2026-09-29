SELECT carrier, MAX(cost) AS max_cost, SUM(cost) AS total_cost FROM shipments WHERE status <> 'LOST' AND carrier IS NULL AND order_id IS NOT NULL GROUP BY 1 ORDER BY max_cost;
