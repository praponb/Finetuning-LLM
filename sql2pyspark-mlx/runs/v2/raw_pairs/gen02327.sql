SELECT
    p.id,
    p.stock_qty,
    s.rating,
    s.city,
    s.supplier_name
FROM products p
INNER JOIN suppliers s ON p.supplier_id = s.id
