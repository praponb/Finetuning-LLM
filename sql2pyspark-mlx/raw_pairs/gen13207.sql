SELECT
    id,
    launch_date,
    category,
    cost,
    SUM(cost) OVER (PARTITION BY MONTH(launch_date), category ORDER BY launch_date, id) AS running_cost
FROM products
ORDER BY cost DESC
