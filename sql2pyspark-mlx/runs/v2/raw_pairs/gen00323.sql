SELECT s.id, s.city FROM stores s WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.store_id = s.id) ORDER BY s.city
