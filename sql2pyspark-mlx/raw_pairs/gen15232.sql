SELECT
    ss.id,
    ss.store_id,
    ss.units,
    p.category
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
WHERE p.supplier_id IS NULL
