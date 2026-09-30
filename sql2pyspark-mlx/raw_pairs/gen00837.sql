SELECT CASE WHEN stock_qty >= 345 THEN 'high' WHEN stock_qty >= 78 THEN 'mid' ELSE 'low' END stock_qty_bucket, COUNT(DISTINCT cost) unique_categorys, ROUND(STDDEV(stock_qty), 2) stddev_stock_qty
FROM products
GROUP BY 1
ORDER BY stddev_stock_qty DESC, stock_qty_bucket
LIMIT 1;
