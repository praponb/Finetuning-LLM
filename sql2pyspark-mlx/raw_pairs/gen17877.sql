SELECT s.* FROM stores s WHERE EXISTS (SELECT 1 FROM inventory c WHERE c.store_id = s.id AND c.last_restock >= s.opened_date);
