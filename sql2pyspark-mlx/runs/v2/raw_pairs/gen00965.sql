SELECT
    id,
    brand,
    launch_date,
    cost,
    LEAD(cost, 1) OVER (PARTITION BY brand ORDER BY launch_date, id) AS next_cost
FROM products
ORDER BY brand
