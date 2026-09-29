SELECT
    i.id,
    i.product_id,
    i.quantity,
    p.supplier_id
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
WHERE p.supplier_id IN (33, 35)
ORDER BY quantity DESC, id
LIMIT 1;
