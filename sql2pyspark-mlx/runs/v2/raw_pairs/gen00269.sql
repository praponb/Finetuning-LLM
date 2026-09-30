SELECT
    id,
    order_id,
    cost,
    status,
    delivered_date,
    ROUND(cost / SUM(cost) OVER (PARTITION BY order_id), 3) AS cost_share,
    SUM(cost) OVER (PARTITION BY status ORDER BY delivered_date, id) AS running_cost
FROM shipments;
