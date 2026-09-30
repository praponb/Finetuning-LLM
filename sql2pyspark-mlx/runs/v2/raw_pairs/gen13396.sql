SELECT s.id, s.sqft, s.city FROM stores s WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.store_id = s.id AND o.channel = 'mobile')
