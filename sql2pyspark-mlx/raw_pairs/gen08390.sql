SELECT
    category,
    MIN(launch_date) AS min_launch_date,
    SUM(CASE WHEN category = 'Books' THEN 1 ELSE 0 END) AS books_count,
    SUM(cost) AS total_cost
FROM products
GROUP BY category;
