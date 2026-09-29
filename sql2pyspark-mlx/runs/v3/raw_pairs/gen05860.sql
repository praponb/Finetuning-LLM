SELECT
    ss.units,
    p.supplier_id,
    p.launch_date,
    s.store_name,
    s.opened_date
FROM store_sales ss
INNER JOIN products p ON p.id = ss.product_id
LEFT OUTER JOIN stores s ON ss.store_id = s.id
WHERE p.product_name LIKE '%Lapto%'
ORDER BY 1 DESC;
