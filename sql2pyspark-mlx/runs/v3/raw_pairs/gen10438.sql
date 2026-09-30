SELECT
    ss.id,
    ss.units,
    p.category,
    p.brand
FROM store_sales ss
LEFT JOIN products p ON ss.product_id = p.id
WHERE ss.store_id IN (2, 3, 4, 5)
