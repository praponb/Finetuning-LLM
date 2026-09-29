SELECT store_sales.id, store_sales.store_id
FROM store_sales
WHERE store_sales.product_id IN (SELECT id FROM products WHERE UPPER(TRIM(product_name)) = 'LAPTOP PRO 14')
ORDER BY store_sales.store_id DESC, id
