SELECT i.id, i.quantity, i.store_id, p.product_name, p.launch_date FROM inventory i INNER JOIN products p ON i.product_id = p.id WHERE i.quantity >= 205 ORDER BY product_name DESC, id LIMIT 10;
