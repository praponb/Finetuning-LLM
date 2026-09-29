SELECT
    s.id,
    s.country,
    s.rating,
    s.supplier_name
FROM suppliers s
WHERE s.id IN (SELECT supplier_id FROM products WHERE category = 'Sports')
ORDER BY 1 DESC
