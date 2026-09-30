SELECT i.product_id, i.last_restock, p.launch_date, s.region FROM inventory i FULL OUTER JOIN products p ON i.product_id = p.id JOIN stores s ON s.id = i.store_id ORDER BY i.last_restock
