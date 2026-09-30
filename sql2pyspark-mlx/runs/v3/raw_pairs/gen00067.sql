SELECT s.region, s.city, i.store_id, i.quantity, i.last_restock FROM stores s LEFT JOIN inventory i ON i.store_id = s.id ORDER BY last_restock, i.store_id
