SELECT
    s.supplier_name,
    s.country,
    p.launch_date,
    p.brand,
    p.stock_qty
FROM suppliers s
INNER JOIN products p ON s.id = p.supplier_id
WHERE NOT (p.price < 720 AND p.category = 'Fashion');
