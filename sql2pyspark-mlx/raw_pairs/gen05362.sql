SELECT s.id, s.city
FROM stores s
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.store_id = s.id AND o.discount < 0.3)
