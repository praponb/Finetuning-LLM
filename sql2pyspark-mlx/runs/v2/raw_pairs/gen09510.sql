SELECT
    i.id,
    i.quantity,
    i.store_id,
    p.price,
    s.supplier_name,
    s.country
FROM inventory i
JOIN products p ON i.product_id = p.id
JOIN suppliers s ON p.supplier_id = s.id
ORDER BY i.quantity, id
LIMIT 5;
