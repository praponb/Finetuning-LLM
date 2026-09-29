SELECT
    i.id,
    i.last_restock,
    i.quantity,
    s.store_name,
    s.region,
    p.brand
FROM inventory i
RIGHT JOIN stores s ON i.store_id = s.id
JOIN products p ON i.product_id = p.id
WHERE p.price <= 800;
