SELECT
    i.id,
    i.product_id,
    s.opened_date,
    s.region,
    p.price,
    p.supplier_id
FROM inventory i
LEFT JOIN stores s ON i.store_id = s.id
RIGHT JOIN products p ON i.product_id = p.id
WHERE i.product_id IN (2, 7);
