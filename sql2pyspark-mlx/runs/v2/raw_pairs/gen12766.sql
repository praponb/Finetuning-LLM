SELECT products.product_name, products.category, store_sales.product_id
FROM products
LEFT JOIN store_sales ON products.id = store_sales.product_id
WHERE products.price <= 600 AND store_sales.product_id IN (5, 8, 9, 10)
