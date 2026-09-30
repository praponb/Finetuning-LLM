SELECT s.id, s.opened_date, s.city, s.store_name FROM stores s WHERE EXISTS (SELECT 1 FROM orders o WHERE o.store_id = s.id AND o.channel IS NOT NULL) ORDER BY id DESC
