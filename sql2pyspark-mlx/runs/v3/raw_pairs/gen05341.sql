SELECT
    i.id,
    i.last_restock,
    s.city,
    p.supplier_id,
    UPPER(TRIM(p.product_name)) AS product_name_normalized
FROM inventory i
INNER JOIN stores s ON i.store_id = s.id
INNER JOIN products p ON i.product_id = p.id
WHERE i.store_id IN (1, 3, 4, 5)
ORDER BY s.city, i.id;
