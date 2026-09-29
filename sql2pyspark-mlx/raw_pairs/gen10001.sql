SELECT
    products.id,
    products.category,
    products.launch_date,
    products.price
FROM products
WHERE products.stock_qty IS NOT NULL AND NOT EXISTS (SELECT 1 FROM inventory WHERE inventory.product_id = products.id AND inventory.store_id IN (1, 2, 5, 6))
