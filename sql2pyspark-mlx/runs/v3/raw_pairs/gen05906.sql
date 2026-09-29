SELECT CASE WHEN p.stock_qty >= 365 THEN 'high' WHEN p.stock_qty >= 195 THEN 'mid' ELSE 'low' END AS stock_qty_bucket, COUNT(ss.units) AS units_count, MIN(ss.sale_date) AS first_sale_date
FROM store_sales ss
LEFT OUTER JOIN products p ON ss.product_id = p.id
GROUP BY CASE WHEN p.stock_qty >= 365 THEN 'high' WHEN p.stock_qty >= 195 THEN 'mid' ELSE 'low' END
ORDER BY stock_qty_bucket ASC
