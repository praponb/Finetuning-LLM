SELECT
    DATE_FORMAT(launch_date, 'yyyy-MM') AS launch_month,
    category,
    COUNT(*) AS total_count,
    COUNT(price) AS price_count
FROM products
WHERE price <= 750 AND brand = 'Umbrella' AND category IN ('Home', 'Electronics')
GROUP BY DATE_FORMAT(launch_date, 'yyyy-MM'), category
ORDER BY total_count, price_count DESC, launch_month, category
LIMIT 50;
