SELECT i.id, i.quantity FROM inventory i WHERE i.store_id IN (SELECT id FROM stores WHERE opened_date >= '2024-02-22') ORDER BY i.quantity DESC, i.id DESC
