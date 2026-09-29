SELECT s.id, s.store_name, s.sqft, s.opened_date FROM stores s WHERE s.sqft <= 13537 AND NOT EXISTS (SELECT 1 FROM store_sales ss WHERE ss.store_id = s.id) ORDER BY s.opened_date, id LIMIT 5;
