SELECT
    brand,
    SUM(stock_qty) AS total_stock_qty,
    COUNT(*) AS cnt,
    SUM(SUM(stock_qty)) OVER (ORDER BY brand) AS cumulative_stock_qty
FROM products
WHERE brand <> 'Initech'
GROUP BY brand
ORDER BY cumulative_stock_qty DESC, brand
LIMIT 10;
