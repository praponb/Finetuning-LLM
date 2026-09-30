SELECT
    i.id,
    i.product_id,
    i.last_restock,
    p.category,
    s.supplier_name,
    s.city
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
