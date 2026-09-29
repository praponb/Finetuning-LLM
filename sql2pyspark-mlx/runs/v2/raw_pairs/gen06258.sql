SELECT id, product_id, unit_price, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price DESC, id) rn FROM order_items WHERE product_id IN (17, 21);
