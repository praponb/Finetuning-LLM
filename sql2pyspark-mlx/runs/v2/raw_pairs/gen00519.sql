SELECT i.id, i.store_id, i.product_id
FROM inventory i
WHERE i.store_id NOT IN (SELECT id FROM stores WHERE store_name LIKE '%Mall%')
ORDER BY store_id ASC, product_id DESC
