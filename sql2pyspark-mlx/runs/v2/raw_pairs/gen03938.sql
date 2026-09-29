SELECT
    i.id,
    i.store_id,
    i.quantity,
    p.launch_date,
    s.supplier_name,
    s.city
FROM inventory i
INNER JOIN products p ON p.id = i.product_id
LEFT JOIN suppliers s ON s.id = p.supplier_id
WHERE i.product_id IN (5, 6, 8) AND LENGTH(p.product_name) < 13;
