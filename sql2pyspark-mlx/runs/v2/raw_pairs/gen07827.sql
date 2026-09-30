SELECT store_sales.id, store_sales.store_id
FROM store_sales
WHERE store_sales.product_id IN (SELECT id FROM products WHERE brand IN ('Globex', 'Stark', 'Acme'))
