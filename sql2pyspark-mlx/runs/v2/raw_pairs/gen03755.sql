SELECT ss.id, ss.store_id
FROM store_sales ss
WHERE ss.product_id IN (3, 8, 10) AND ss.product_id NOT IN (SELECT id FROM products WHERE cost >= 600)
ORDER BY ss.store_id DESC, id
