SELECT p.id, p.cost FROM products p WHERE p.category NOT IN ('Grocery', 'Books', 'Sports') AND p.id IN (SELECT product_id FROM order_items WHERE product_id = 21);
