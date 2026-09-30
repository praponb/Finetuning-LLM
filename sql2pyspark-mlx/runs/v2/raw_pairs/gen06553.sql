SELECT
    ss.id,
    ss.store_id,
    ss.units,
    p.product_name,
    p.launch_date,
    s.city,
    s.rating
FROM store_sales ss
FULL OUTER JOIN products p ON ss.product_id = p.id
LEFT JOIN suppliers s ON p.supplier_id = s.id
WHERE ss.store_id = 2
