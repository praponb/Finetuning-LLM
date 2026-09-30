SELECT
    i.id,
    i.product_id,
    i.last_restock,
    i.store_id,
    p.category
FROM inventory i
JOIN products p ON i.product_id = p.id
WHERE i.store_id IN (1, 3, 4, 6) OR p.price <= 740;
