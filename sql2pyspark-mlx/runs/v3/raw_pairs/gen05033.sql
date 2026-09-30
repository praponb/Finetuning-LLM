SELECT id, order_id, cost, delivered_date, ROUND(cost / SUM(cost) OVER (PARTITION BY order_id), 3) AS cost_share, LEAD(cost, 1) OVER (ORDER BY delivered_date, id) AS next_cost FROM shipments;
