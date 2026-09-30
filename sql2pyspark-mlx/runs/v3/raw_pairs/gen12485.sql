SELECT
    i.id,
    i.store_id,
    i.product_id,
    p.launch_date,
    p.price,
    s.supplier_name,
    s.country
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE i.store_id IN (2, 4, 5, 6) AND p.category = 'Grocery'
ORDER BY p.price DESC, s.supplier_name;
