SELECT
    id,
    category,
    launch_date,
    DENSE_RANK() OVER (PARTITION BY category ORDER BY launch_date) AS dense_rnk
FROM products
WHERE brand IN ('Umbrella', 'Acme', 'Stark')
ORDER BY category DESC;
