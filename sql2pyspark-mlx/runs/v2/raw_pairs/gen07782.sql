SELECT quantity, product_id, order_id FROM order_items WHERE (order_id = 27 AND unit_price BETWEEN 150 AND 340) OR quantity != 5 ORDER BY quantity DESC, 2 DESC;
