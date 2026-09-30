SELECT
    id,
    order_id,
    cost,
    AVG(cost) OVER (PARTITION BY order_id) AS order_id_avg_cost
FROM shipments
