SELECT
    p.launch_date,
    p.stock_qty,
    p.category,
    s.city
FROM products p
LEFT OUTER JOIN suppliers s ON p.supplier_id = s.id;
