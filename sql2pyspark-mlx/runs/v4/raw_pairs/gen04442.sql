SELECT CASE WHEN stock_qty >= 430 THEN 'high' WHEN stock_qty >= 141 THEN 'mid' ELSE 'low' END AS stock_qty_bucket, COUNT(stock_qty) AS stock_qty_count
FROM products
GROUP BY stock_qty_bucket
HAVING COUNT(stock_qty) >= 4
