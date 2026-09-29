SELECT
    ss.id,
    ss.store_id,
    p.launch_date,
    p.brand,
    s.opened_date
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
LEFT JOIN stores s ON ss.store_id = s.id;
