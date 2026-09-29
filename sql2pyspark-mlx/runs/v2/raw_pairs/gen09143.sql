SELECT a.id, a.store_id, a.quantity, a.last_restock FROM inventory a WHERE a.quantity = (SELECT MAX(b.quantity) FROM inventory b WHERE b.store_id = a.store_id)
