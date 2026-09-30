SELECT s.id, s.store_name
FROM stores s
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.store_id = s.id AND o.customer_id IS NULL);
