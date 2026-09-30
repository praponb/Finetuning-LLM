SELECT products.id, products.launch_date FROM products WHERE products.id IN (SELECT product_id FROM order_items WHERE unit_price BETWEEN 310 AND 370) ORDER BY products.launch_date, id LIMIT 3
