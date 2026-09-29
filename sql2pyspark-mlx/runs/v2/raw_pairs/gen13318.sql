SELECT
    i.last_restock,
    p.brand,
    p.category,
    s.city,
    s.supplier_name
FROM inventory i
LEFT JOIN products p ON i.product_id = p.id
RIGHT JOIN suppliers s ON p.supplier_id = s.id
ORDER BY s.supplier_name
