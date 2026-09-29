SELECT products.id, products.brand
FROM products
WHERE NOT EXISTS (SELECT 1 FROM store_sales WHERE store_sales.product_id = products.id)
ORDER BY id DESC;
