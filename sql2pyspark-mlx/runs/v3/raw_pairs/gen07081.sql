SELECT
    id,
    category,
    launch_date,
    RANK() OVER (PARTITION BY category ORDER BY launch_date) AS rnk
FROM products
ORDER BY rnk;
