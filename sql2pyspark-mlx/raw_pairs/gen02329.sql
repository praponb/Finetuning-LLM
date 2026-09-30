SELECT
    i.product_id,
    p.brand,
    p.supplier_id,
    p.cost
FROM inventory i
JOIN products p ON p.id = i.product_id
WHERE i.product_id = 2;
