SELECT
    p.id,
    p.launch_date,
    s.city,
    s.supplier_name
FROM products p
JOIN suppliers s ON p.supplier_id = s.id
WHERE p.stock_qty > 183;
