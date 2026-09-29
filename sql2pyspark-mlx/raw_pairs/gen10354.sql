SELECT a.id, a.store_id, a.quantity FROM inventory a WHERE a.quantity = (SELECT MIN(b.quantity) FROM inventory b WHERE b.store_id = a.store_id)
