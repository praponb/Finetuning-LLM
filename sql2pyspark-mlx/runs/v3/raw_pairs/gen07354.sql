SELECT id, order_id, status, cost, ROUND(cost / SUM(cost) OVER (PARTITION BY order_id, status), 3) AS cost_share FROM shipments;
