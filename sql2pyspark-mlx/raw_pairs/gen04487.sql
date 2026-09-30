SELECT
    id,
    delivered_date,
    cost,
    LAG(cost, 1, 0) OVER (ORDER BY delivered_date, id) AS prev_cost
FROM shipments;
