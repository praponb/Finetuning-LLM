SELECT
    p.id,
    p.price,
    p.launch_date,
    p.product_name
FROM products p
WHERE EXISTS (SELECT 1 FROM inventory i WHERE i.product_id = p.id)
ORDER BY p.price;
