SELECT
    ss.product_id,
    p.cost,
    p.launch_date,
    s.supplier_name,
    s.country
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
LEFT JOIN suppliers s ON p.supplier_id = s.id
ORDER BY supplier_name
