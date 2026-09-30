SELECT i.id, i.last_restock, i.quantity, i.product_id, s.store_name, s.city, s.region FROM inventory i LEFT JOIN stores s ON i.store_id = s.id ORDER BY i.last_restock ASC, id ASC LIMIT 10
