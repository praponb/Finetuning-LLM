SELECT
    s.id,
    s.store_name,
    s.region,
    s.opened_date
FROM stores s
WHERE s.id IN (SELECT store_id FROM orders WHERE channel NOT IN ('mobile', 'web', 'partner'))
ORDER BY s.store_name DESC, opened_date DESC
