SELECT products.id, products.product_name FROM products WHERE EXISTS (SELECT 1 FROM order_items WHERE order_items.product_id = products.id) ORDER BY product_name DESC, id ASC LIMIT 50;
