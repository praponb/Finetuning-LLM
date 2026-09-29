SELECT p.cost, p.supplier_id, p.launch_date, i.last_restock, i.quantity, i.store_id FROM products p JOIN inventory i ON i.product_id = p.id WHERE p.supplier_id IN (10, 13, 19, 22);
