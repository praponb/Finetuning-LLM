SELECT
    s.opened_date,
    s.sqft,
    ss.revenue,
    ss.store_id
FROM stores s
LEFT OUTER JOIN store_sales ss ON s.id = ss.store_id
ORDER BY ss.store_id DESC;
