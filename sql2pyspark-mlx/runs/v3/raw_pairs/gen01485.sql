SELECT
    p.id,
    p.cost,
    p.category,
    p.stock_qty,
    s.city,
    s.supplier_name
FROM products p
LEFT OUTER JOIN suppliers s ON p.supplier_id = s.id
WHERE p.brand NOT IN ('Initech', 'Acme', 'Globex') AND p.stock_qty BETWEEN 172 AND 300
