SELECT
    CASE WHEN price >= 1340 THEN 'high' WHEN price >= 510 THEN 'mid' ELSE 'low' END AS price_bucket,
    category,
    brand,
    AVG(price) AS avg_price,
    MIN(launch_date) AS min_launch_date,
    ROUND(AVG(stock_qty), 2) AS avg_stock_qty
FROM products
GROUP BY CASE WHEN price >= 1340 THEN 'high' WHEN price >= 510 THEN 'mid' ELSE 'low' END, category, brand;
