SELECT CASE WHEN p.stock_qty >= 444 THEN 'high' WHEN p.stock_qty >= 123 THEN 'mid' ELSE 'low' END AS stock_qty_bucket, COUNT(DISTINCT i.quantity) AS unique_products, COUNT(*) AS num_inventory
FROM inventory i
INNER JOIN products p ON p.id = i.product_id
INNER JOIN stores s ON s.id = i.store_id
GROUP BY CASE WHEN p.stock_qty >= 444 THEN 'high' WHEN p.stock_qty >= 123 THEN 'mid' ELSE 'low' END
HAVING COUNT(DISTINCT i.quantity) >= 5
ORDER BY num_inventory DESC;
