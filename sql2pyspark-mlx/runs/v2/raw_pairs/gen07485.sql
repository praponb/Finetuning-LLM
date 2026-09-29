SELECT
    s.store_name,
    s.city,
    s.region,
    ss.sale_date,
    ss.store_id,
    ss.revenue
FROM stores s
LEFT OUTER JOIN store_sales ss ON s.id = ss.store_id
WHERE s.store_name LIKE '%Riversid%';
