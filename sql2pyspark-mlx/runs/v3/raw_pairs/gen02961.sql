SELECT s.id, s.store_name, s.opened_date FROM stores s WHERE s.sqft IS NOT NULL AND NOT EXISTS (SELECT 1 FROM store_sales ss WHERE ss.store_id = s.id) ORDER BY id DESC LIMIT 50;
