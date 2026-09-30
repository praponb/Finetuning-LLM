SELECT s.id, s.opened_date, s.city FROM stores s WHERE LENGTH(s.store_name) >= 14 AND EXISTS (SELECT 1 FROM inventory i WHERE i.store_id = s.id AND i.quantity <= 205) ORDER BY s.city, id LIMIT 3;
