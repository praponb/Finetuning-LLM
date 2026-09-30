SELECT
    ss.id,
    ss.revenue,
    p.cost,
    s.opened_date,
    s.city
FROM store_sales ss
FULL OUTER JOIN products p ON ss.product_id = p.id
FULL OUTER JOIN stores s ON ss.store_id = s.id
