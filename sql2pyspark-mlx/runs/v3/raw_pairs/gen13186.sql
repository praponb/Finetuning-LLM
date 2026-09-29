SELECT
    ss.id,
    ss.store_id,
    ss.revenue,
    s.city
FROM store_sales ss
LEFT JOIN stores s ON s.id = ss.store_id
ORDER BY ss.store_id DESC, id DESC
LIMIT 10;
