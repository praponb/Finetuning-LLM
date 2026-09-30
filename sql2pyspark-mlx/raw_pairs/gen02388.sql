SELECT s.* FROM stores s WHERE NOT EXISTS (SELECT 1 FROM store_sales ch WHERE ch.store_id = s.id AND ch.store_id = 3)
