SELECT store_sales.id, store_sales.units
FROM store_sales
WHERE store_sales.product_id NOT IN (SELECT id FROM products WHERE category = 'Home');
