SELECT id, quantity FROM order_items WHERE quantity < (SELECT MIN(quantity) FROM order_items) ORDER BY 1 DESC
