SELECT
    i.id,
    i.product_id,
    p.brand,
    p.supplier_id
FROM inventory i
JOIN products p ON p.id = i.product_id
WHERE i.product_id IN (2, 7, 8, 9)
