SELECT
    p.id,
    p.category,
    p.launch_date,
    p.price
FROM products p
WHERE p.category IN ('Books', 'Sports') AND NOT EXISTS (SELECT 1 FROM inventory i WHERE i.product_id = p.id AND i.store_id IN (1, 3, 4, 6));
