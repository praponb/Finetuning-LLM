SELECT i.id, i.last_restock, i.quantity, p.launch_date, p.supplier_id, s.store_name FROM inventory i LEFT JOIN products p ON i.product_id = p.id INNER JOIN stores s ON s.id = i.store_id;
