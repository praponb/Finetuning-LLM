SELECT launch_month, price_count
FROM (
    SELECT MONTH(launch_date) AS launch_month, COUNT(price) AS price_count
    FROM products
    WHERE ARRAY_CONTAINS(tags, 'sale')
    GROUP BY MONTH(launch_date)
) AS sub
WHERE price_count >= 7
ORDER BY price_count DESC;
