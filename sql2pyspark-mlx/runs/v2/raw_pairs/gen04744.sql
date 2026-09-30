SELECT
    p.id,
    p.brand,
    p.stock_qty,
    p.supplier_id,
    s.supplier_name,
    s.country
FROM products p
INNER JOIN suppliers s ON s.id = p.supplier_id
ORDER BY id, country
