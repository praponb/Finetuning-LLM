SELECT
    ss.id,
    ss.store_id,
    ss.sale_date,
    p.brand,
    s.opened_date,
    s.region
FROM store_sales ss
JOIN products p ON ss.product_id = p.id
JOIN stores s ON ss.store_id = s.id
ORDER BY s.region DESC, 3;
