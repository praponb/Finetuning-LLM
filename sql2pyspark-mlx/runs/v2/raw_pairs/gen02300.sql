SELECT
    s.rating,
    s.supplier_name,
    p.stock_qty,
    p.price,
    p.launch_date,
    CASE WHEN s.country = 'DE' THEN 1 ELSE 0 END AS is_de
FROM suppliers s
LEFT JOIN products p ON p.supplier_id = s.id
ORDER BY launch_date DESC
