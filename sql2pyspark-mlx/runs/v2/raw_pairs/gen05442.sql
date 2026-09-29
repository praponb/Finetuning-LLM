SELECT
    ss.id,
    ss.sale_date,
    ss.units,
    s.opened_date,
    s.region,
    p.launch_date
FROM store_sales ss
LEFT OUTER JOIN stores s ON ss.store_id = s.id
JOIN products p ON ss.product_id = p.id
WHERE ss.units < 13
ORDER BY s.opened_date ASC, id DESC
LIMIT 50
