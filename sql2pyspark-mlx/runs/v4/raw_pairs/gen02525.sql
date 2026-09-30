SELECT
    ss.id,
    ss.units,
    ss.store_id,
    p.product_name,
    p.supplier_id,
    s.city,
    s.store_name
FROM store_sales ss
RIGHT OUTER JOIN products p ON ss.product_id = p.id
LEFT OUTER JOIN stores s ON ss.store_id = s.id
WHERE YEAR(s.opened_date) >= 2025 OR ss.product_id IN (4, 5)
ORDER BY ss.id DESC
