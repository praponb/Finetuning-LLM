SELECT
    s.id,
    s.city,
    s.opened_date,
    s.store_name
FROM stores s
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.store_id = s.id AND o.channel IN ('store', 'web'));
