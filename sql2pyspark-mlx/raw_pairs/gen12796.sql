SELECT s.id, s.sqft FROM stores s WHERE EXISTS (SELECT 1 FROM orders o WHERE o.store_id = s.id AND o.customer_id = 1) ORDER BY s.sqft DESC;
