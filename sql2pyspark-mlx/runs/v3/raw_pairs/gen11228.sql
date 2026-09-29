SELECT
    brand,
    MAX(price) - MIN(price) AS price_range,
    ROUND(STDDEV(price), 2) AS stddev_price,
    AVG(price) AS avg_price
FROM products
WHERE brand = 'Stark' AND stock_qty <> 176 AND product_name LIKE 'B%'
GROUP BY brand
HAVING COUNT(*) >= 5
