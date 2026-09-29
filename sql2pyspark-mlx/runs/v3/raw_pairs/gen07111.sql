SELECT
    id,
    order_id,
    cost,
    carrier,
    AVG(cost) OVER (PARTITION BY order_id) AS order_id_avg_cost,
    MIN(cost) OVER (PARTITION BY carrier) AS carrier_min_cost
FROM shipments;
