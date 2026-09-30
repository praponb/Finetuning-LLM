SELECT
    i.quantity,
    p.supplier_id,
    p.category,
    s.city,
    s.supplier_name
FROM inventory i
JOIN products p ON i.product_id = p.id
JOIN suppliers s ON p.supplier_id = s.id
ORDER BY p.supplier_id DESC;
