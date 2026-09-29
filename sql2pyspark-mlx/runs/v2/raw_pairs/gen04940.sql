SELECT
    id,
    brand,
    launch_date,
    cost,
    cost - LAG(cost, 1) OVER (PARTITION BY brand ORDER BY launch_date, id) AS cost_change
FROM products;
