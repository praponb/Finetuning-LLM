SELECT
    i.id,
    i.product_id,
    i.quantity,
    p.supplier_id,
    s.country
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
