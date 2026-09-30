SELECT s.id, s.city, s.sqft, s.opened_date FROM stores s WHERE EXISTS (SELECT 1 FROM store_sales ss WHERE ss.store_id = s.id AND ss.revenue > 2900) ORDER BY id DESC;
