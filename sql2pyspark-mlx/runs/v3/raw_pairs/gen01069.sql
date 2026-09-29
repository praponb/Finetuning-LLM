SELECT ss.id, ss.units
FROM store_sales ss
WHERE ss.store_id IN (1, 3, 4, 6) AND ss.product_id IN (SELECT id FROM products WHERE supplier_id IS NOT NULL)
ORDER BY 1 DESC
