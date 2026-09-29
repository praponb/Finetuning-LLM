SELECT
    s.id,
    s.opened_date,
    s.store_name,
    s.city
FROM stores s
WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.store_id = s.id AND o.discount > 0.19);
