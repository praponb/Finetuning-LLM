SELECT
    id,
    order_id,
    delivered_date,
    cost,
    LAG(cost, 1) OVER (PARTITION BY order_id ORDER BY delivered_date, id) AS prev_cost
FROM shipments
ORDER BY id
