SELECT
    i.id,
    i.quantity,
    i.store_id,
    p.product_name,
    p.brand,
    s.supplier_name,
    s.city
FROM inventory i
JOIN products p ON i.product_id = p.id
JOIN suppliers s ON s.id = p.supplier_id
WHERE i.store_id IN (3, 4, 5)
ORDER BY s.city
