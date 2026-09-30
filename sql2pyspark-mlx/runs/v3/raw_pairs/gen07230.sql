SELECT
    ss.revenue,
    ss.units,
    ss.sale_date,
    s.opened_date,
    s.store_name
FROM store_sales ss
RIGHT JOIN stores s ON ss.store_id = s.id
