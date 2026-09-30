SELECT
    id,
    category,
    launch_date,
    cost,
    LAG(cost, 2) OVER (PARTITION BY category ORDER BY launch_date, id) AS prev_cost
FROM products
WHERE cost >= 320
