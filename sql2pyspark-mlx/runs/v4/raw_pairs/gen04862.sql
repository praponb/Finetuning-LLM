SELECT CASE WHEN p.stock_qty >= 421 THEN 'high' WHEN p.stock_qty >= 52 THEN 'mid' ELSE 'low' END stock_qty_bucket, p.category, ROUND(STDDEV(ss.revenue), 2) stddev_revenue
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
WHERE p.supplier_id = 28
GROUP BY CASE WHEN p.stock_qty >= 421 THEN 'high' WHEN p.stock_qty >= 52 THEN 'mid' ELSE 'low' END, p.category
ORDER BY stddev_revenue, stock_qty_bucket DESC, category
LIMIT 10;
