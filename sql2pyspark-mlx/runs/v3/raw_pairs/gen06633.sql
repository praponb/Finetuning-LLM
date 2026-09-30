SELECT
    p.id,
    p.product_name,
    p.supplier_id,
    s.rating
FROM products p
LEFT OUTER JOIN suppliers s ON s.id = p.supplier_id
WHERE s.country <> 'JP'
ORDER BY product_name
