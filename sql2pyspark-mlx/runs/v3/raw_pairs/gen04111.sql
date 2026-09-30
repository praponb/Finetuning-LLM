SELECT p.id, p.cost, p.launch_date, p.category FROM products p WHERE p.stock_qty != 102 AND p.id IN (SELECT product_id FROM order_items WHERE quantity = 6) ORDER BY p.launch_date ASC;
