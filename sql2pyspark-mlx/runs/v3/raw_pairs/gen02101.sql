SELECT p.id, p.launch_date FROM products p WHERE p.id IN (SELECT product_id FROM order_items);
