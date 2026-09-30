SELECT s.id, s.store_name, s.city FROM stores s WHERE s.city IN ('London', 'Bangkok') AND EXISTS (SELECT 1 FROM store_sales ss WHERE ss.store_id = s.id)
