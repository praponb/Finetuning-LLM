SELECT i.id, i.store_id FROM inventory i WHERE i.store_id IN (SELECT id FROM stores WHERE UPPER(store_name) LIKE '%IRP%') ORDER BY i.id DESC;
