SELECT
    i.id,
    i.last_restock,
    i.quantity,
    i.product_id,
    s.city
FROM inventory i
JOIN stores s ON i.store_id = s.id
WHERE i.store_id IN (1, 4);
