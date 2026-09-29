SELECT
    s.country,
    s.city,
    p.stock_qty,
    p.price,
    p.launch_date
FROM suppliers s
LEFT JOIN products p ON p.supplier_id = s.id
WHERE p.cost <= 190;
