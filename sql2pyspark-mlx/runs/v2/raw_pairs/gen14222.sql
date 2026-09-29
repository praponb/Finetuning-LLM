SELECT * FROM (SELECT id, order_id, quantity, product_id, RANK() OVER (PARTITION BY order_id ORDER BY quantity DESC) AS row_num FROM order_items) AS r WHERE row_num <= 3
