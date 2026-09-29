SELECT
    brand,
    DATE_FORMAT(launch_date, 'yyyy-MM') AS launch_month,
    COUNT(price) AS price_count,
    ROUND(AVG(price), 2) AS avg_price
FROM products
WHERE brand = 'Initech' AND supplier_id IS NOT NULL
GROUP BY 1, 2
ORDER BY price_count ASC
