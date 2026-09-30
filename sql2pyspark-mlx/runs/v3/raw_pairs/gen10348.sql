SELECT
    i.id,
    i.quantity,
    i.last_restock,
    p.product_name,
    p.cost,
    s.supplier_name
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
RIGHT JOIN suppliers s ON p.supplier_id = s.id
WHERE i.quantity < 185
ORDER BY s.supplier_name DESC, i.id ASC;
