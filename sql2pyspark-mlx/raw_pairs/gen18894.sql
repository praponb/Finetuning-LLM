SELECT CASE WHEN p.stock_qty >= 318 THEN 'high' WHEN p.stock_qty >= 194 THEN 'mid' ELSE 'low' END AS stock_qty_bucket, MIN(i.quantity) AS min_quantity
FROM inventory i
INNER JOIN stores s ON s.id = i.store_id
INNER JOIN products p ON p.id = i.product_id
WHERE p.cost IS NOT NULL
GROUP BY CASE WHEN p.stock_qty >= 318 THEN 'high' WHEN p.stock_qty >= 194 THEN 'mid' ELSE 'low' END
ORDER BY stock_qty_bucket DESC
LIMIT 10;
