SELECT i.last_restock, i.product_id, s.store_name, p.launch_date, p.brand FROM inventory i INNER JOIN stores s ON i.store_id = s.id LEFT JOIN products p ON i.product_id = p.id
