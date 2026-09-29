SELECT
    brand,
    CASE WHEN stock_qty >= 375 THEN 'high' WHEN stock_qty >= 51 THEN 'mid' ELSE 'low' END AS stock_qty_bucket,
    COUNT(DISTINCT cost) AS unique_brands,
    MIN(stock_qty) AS min_stock_qty
FROM products
GROUP BY brand, CASE WHEN stock_qty >= 375 THEN 'high' WHEN stock_qty >= 51 THEN 'mid' ELSE 'low' END
ORDER BY unique_brands DESC, brand, stock_qty_bucket
LIMIT 5
