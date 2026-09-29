SELECT ss.id, ss.units
FROM store_sales ss
WHERE ss.revenue BETWEEN 1600 AND 3600 AND ss.product_id IN (SELECT id FROM products WHERE cost >= 680)
ORDER BY ss.id, units
