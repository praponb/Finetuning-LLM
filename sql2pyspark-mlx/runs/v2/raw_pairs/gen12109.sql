SELECT
    i.quantity,
    i.store_id,
    s.store_name,
    s.city,
    p.cost,
    p.category
FROM inventory i
JOIN stores s ON s.id = i.store_id
JOIN products p ON i.product_id = p.id
WHERE i.store_id IN (4, 5)
ORDER BY store_id, quantity DESC;
