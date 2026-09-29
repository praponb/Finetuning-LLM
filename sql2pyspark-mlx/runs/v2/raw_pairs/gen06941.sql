SELECT id, product_id, quantity FROM (SELECT id, product_id, quantity, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity, id) AS row_num FROM order_items) AS r WHERE row_num = 1;
