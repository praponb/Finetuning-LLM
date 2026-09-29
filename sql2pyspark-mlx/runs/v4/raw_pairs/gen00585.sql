SELECT
    ss.id,
    ss.revenue,
    ss.units,
    s.opened_date,
    p.price,
    p.product_name
FROM store_sales ss
INNER JOIN stores s ON ss.store_id = s.id
INNER JOIN products p ON p.id = ss.product_id;
