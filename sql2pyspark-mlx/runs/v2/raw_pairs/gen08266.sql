SELECT p.id, p.price, p.cost, p.product_name FROM products p WHERE p.id IN (SELECT product_id FROM order_items) ORDER BY p.price
