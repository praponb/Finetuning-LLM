SELECT s.id, s.store_name FROM stores s WHERE s.id IN (SELECT store_id FROM inventory) ORDER BY s.id DESC
