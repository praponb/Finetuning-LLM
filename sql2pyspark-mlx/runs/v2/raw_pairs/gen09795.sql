SELECT
    i.store_id,
    i.product_id,
    p.supplier_id,
    s.store_name
FROM inventory i
JOIN products p ON p.id = i.product_id
JOIN stores s ON s.id = i.store_id
