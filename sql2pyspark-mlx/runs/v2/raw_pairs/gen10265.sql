SELECT i.id, i.last_restock, i.product_id, p.product_name, s.store_name, s.sqft FROM inventory i LEFT JOIN products p ON i.product_id = p.id JOIN stores s ON i.store_id = s.id;
