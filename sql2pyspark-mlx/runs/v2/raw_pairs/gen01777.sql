SELECT
    ss.id,
    ss.revenue,
    s.region,
    s.city
FROM store_sales ss
INNER JOIN stores s ON ss.store_id = s.id
ORDER BY revenue DESC, id ASC
LIMIT 20;
