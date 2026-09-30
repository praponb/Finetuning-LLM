SELECT i.id, i.last_restock FROM inventory i WHERE i.store_id NOT IN (SELECT id FROM stores WHERE city IN ('Berlin', 'New York', 'Tokyo')) ORDER BY 2, id DESC
