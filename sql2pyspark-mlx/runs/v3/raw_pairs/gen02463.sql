SELECT i.id, i.last_restock, i.quantity FROM inventory i WHERE i.store_id IN (SELECT id FROM stores WHERE opened_date > '2024-05-06')
