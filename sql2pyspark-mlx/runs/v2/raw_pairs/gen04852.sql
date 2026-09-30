SELECT
    s.id,
    s.region,
    s.opened_date,
    s.city
FROM stores s
WHERE EXISTS (SELECT 1 FROM store_sales ss WHERE ss.store_id = s.id)
ORDER BY s.city;
