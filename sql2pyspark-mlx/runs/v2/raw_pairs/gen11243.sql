SELECT i.id, i.last_restock FROM inventory i WHERE i.store_id IN (SELECT id FROM stores WHERE opened_date <= '2025-02-09') ORDER BY i.last_restock, i.id DESC LIMIT 10;
