SELECT
    ss.id,
    ss.store_id,
    p.category,
    p.price,
    s.city
FROM store_sales ss
LEFT OUTER JOIN products p ON ss.product_id = p.id
INNER JOIN suppliers s ON s.id = p.supplier_id
WHERE LENGTH(s.supplier_name) < 11 AND p.price IS NOT NULL
ORDER BY p.category, store_id DESC
