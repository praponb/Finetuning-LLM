SELECT
    id,
    order_id,
    carrier,
    cost,
    AVG(cost) OVER (PARTITION BY order_id, carrier) AS order_id_avg_cost
FROM shipments
ORDER BY order_id_avg_cost DESC, 2 DESC;
