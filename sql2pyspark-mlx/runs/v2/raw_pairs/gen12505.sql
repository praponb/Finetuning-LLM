SELECT
    s.id,
    s.city,
    s.opened_date,
    s.region
FROM stores s
WHERE s.id IN (SELECT store_id FROM orders);
