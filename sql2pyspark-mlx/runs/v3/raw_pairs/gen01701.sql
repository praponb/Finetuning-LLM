SELECT store_sales.id, store_sales.store_id
FROM store_sales
WHERE store_sales.store_id IN (1, 2, 5, 6) AND store_sales.product_id IN (SELECT id FROM products WHERE brand = 'Stark')
ORDER BY id DESC;
