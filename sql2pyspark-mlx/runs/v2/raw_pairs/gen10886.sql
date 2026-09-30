SELECT ss.id, ss.store_id, ss.units
FROM store_sales ss
WHERE ss.product_id IN (3, 6, 8, 9) AND ss.product_id IN (SELECT id FROM products WHERE ARRAY_CONTAINS(tags, 'sale'))
