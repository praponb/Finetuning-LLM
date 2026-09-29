SELECT
    p.cost,
    p.stock_qty,
    p.category,
    s.city,
    s.supplier_name,
    s.country
FROM products p
JOIN suppliers s ON p.supplier_id = s.id
WHERE s.country IN ('SG', 'US', 'JP')
