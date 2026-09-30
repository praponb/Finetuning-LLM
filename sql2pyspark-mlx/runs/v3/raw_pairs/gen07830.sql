SELECT s.id, s.opened_date
FROM stores s
WHERE s.city != 'New York' AND NOT EXISTS (SELECT 1 FROM orders o WHERE o.store_id = s.id AND o.customer_id IS NOT NULL);
