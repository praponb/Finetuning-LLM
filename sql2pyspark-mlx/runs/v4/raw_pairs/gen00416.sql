SELECT s.id, s.region, s.opened_date FROM stores s WHERE NOT EXISTS (SELECT 1 FROM inventory i WHERE i.store_id = s.id AND i.product_id IN (7, 9)) ORDER BY s.id DESC;
