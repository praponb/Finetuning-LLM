SELECT
    s.region,
    s.opened_date,
    s.store_name,
    ss.store_id
FROM stores s
JOIN store_sales ss ON s.id = ss.store_id
WHERE s.opened_date > '2024-08-01';
