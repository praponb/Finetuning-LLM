SELECT
    brand,
    ROUND(STDDEV(cost), 2) AS stddev_cost,
    AVG(stock_qty) AS avg_stock_qty,
    COUNT(CASE WHEN category = 'Fashion' THEN 1 END) AS fashion_count
FROM products
GROUP BY 1
HAVING AVG(stock_qty) > 170;
