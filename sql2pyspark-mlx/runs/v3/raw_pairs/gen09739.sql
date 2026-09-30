SELECT CASE WHEN stock_qty >= 344 THEN 'high' WHEN stock_qty >= 115 THEN 'mid' ELSE 'low' END stock_qty_bucket, category, MAX(stock_qty) - MIN(stock_qty) stock_qty_range
FROM products
GROUP BY 1, 2
