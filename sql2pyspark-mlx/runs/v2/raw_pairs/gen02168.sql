SELECT
    p.id,
    p.launch_date,
    p.category,
    p.price
FROM products p
WHERE NOT EXISTS (SELECT 1 FROM inventory i WHERE i.product_id = p.id)
