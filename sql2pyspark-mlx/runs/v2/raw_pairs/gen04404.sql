SELECT
    i.product_id,
    i.store_id,
    p.price,
    s.country
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE p.supplier_id IS NOT NULL
