SELECT ss.id, ss.product_id
FROM store_sales ss
WHERE ss.store_id IN (SELECT id FROM stores WHERE city = 'Berlin')
ORDER BY id;
