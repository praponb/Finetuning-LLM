SELECT
    s.city,
    s.region,
    ss.revenue,
    ss.units
FROM stores s
INNER JOIN store_sales ss ON ss.store_id = s.id
WHERE s.region = 'North' AND MONTH(s.opened_date) IN (5, 6, 8);
