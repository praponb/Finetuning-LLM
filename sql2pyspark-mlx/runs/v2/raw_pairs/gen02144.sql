SELECT
    p.id,
    p.brand,
    p.cost,
    p.product_name
FROM products p
WHERE p.category = 'Home' AND NOT EXISTS (SELECT 1 FROM store_sales ss WHERE ss.product_id = p.id)
