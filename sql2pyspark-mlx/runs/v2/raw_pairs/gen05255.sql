SELECT
    p.id,
    p.cost,
    p.price,
    p.launch_date
FROM products p
WHERE NOT EXISTS (SELECT 1 FROM inventory i WHERE i.product_id = p.id AND i.last_restock < '2024-10-28');
