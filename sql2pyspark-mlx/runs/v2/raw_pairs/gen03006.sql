SELECT
    s.opened_date,
    s.city,
    ss.store_id,
    ss.revenue,
    ss.product_id
FROM stores s
LEFT OUTER JOIN store_sales ss ON s.id = ss.store_id
WHERE s.city IN ('Singapore', 'London', 'Bangkok')
ORDER BY store_id DESC;
