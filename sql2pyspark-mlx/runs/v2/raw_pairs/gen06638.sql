SELECT id, order_id, unit_price, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price DESC, id) AS rn FROM order_items WHERE product_id = 13;
