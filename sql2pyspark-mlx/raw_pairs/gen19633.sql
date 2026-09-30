SELECT s.id, s.opened_date, s.sqft
FROM stores s
WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.store_id = s.id)
ORDER BY id DESC;
