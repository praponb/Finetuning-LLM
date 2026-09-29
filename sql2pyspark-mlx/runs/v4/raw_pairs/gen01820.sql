SELECT
    i.id,
    i.last_restock,
    s.store_name,
    p.price
FROM inventory i
JOIN stores s ON i.store_id = s.id
JOIN products p ON i.product_id = p.id
ORDER BY s.store_name, last_restock;
