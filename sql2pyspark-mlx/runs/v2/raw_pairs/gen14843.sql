SELECT
    i.id,
    i.last_restock,
    p.product_name,
    p.category,
    s.country
FROM inventory i
INNER JOIN products p ON p.id = i.product_id
LEFT JOIN suppliers s ON p.supplier_id = s.id
ORDER BY p.category, last_restock;
