SELECT
    products.id,
    products.launch_date,
    products.category,
    products.cost
FROM products
WHERE products.category != 'Sports' AND NOT EXISTS (SELECT 1 FROM store_sales WHERE store_sales.product_id = products.id)
ORDER BY launch_date, category DESC, id
LIMIT 50;
