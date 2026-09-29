SELECT
    category,
    SUM(price) AS total_price,
    MAX(launch_date) AS last_launch_date,
    SUM(CASE WHEN category = 'Fashion' THEN 1 ELSE 0 END) AS fashion_count
FROM products
GROUP BY category
ORDER BY total_price, category
LIMIT 5;
