SELECT
    p.stock_qty,
    p.cost,
    s.city,
    s.supplier_name
FROM products p
LEFT JOIN suppliers s ON p.supplier_id = s.id
WHERE s.city IN ('Bangkok', 'London', 'Berlin')
ORDER BY cost DESC
