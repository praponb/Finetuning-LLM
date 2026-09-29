SELECT s.id, s.opened_date FROM stores s WHERE s.opened_date >= '2024-06-01' AND EXISTS (SELECT 1 FROM orders o WHERE o.store_id = s.id AND o.customer_id = 6)
