SELECT products.id, products.cost, products.product_name
FROM products
WHERE EXISTS (SELECT 1 FROM store_sales WHERE store_sales.product_id = products.id)
