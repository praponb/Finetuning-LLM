SELECT
    products.id,
    products.cost,
    products.launch_date,
    products.category
FROM products
WHERE NOT EXISTS (SELECT 1 FROM store_sales WHERE store_sales.product_id = products.id AND store_sales.product_id IN (1, 3, 7))
