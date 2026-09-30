SELECT
    ss.id,
    ss.store_id,
    ss.units,
    p.price,
    p.supplier_id,
    p.brand
FROM store_sales ss
LEFT JOIN products p ON ss.product_id = p.id
WHERE p.supplier_id IS NULL
ORDER BY p.price DESC;
