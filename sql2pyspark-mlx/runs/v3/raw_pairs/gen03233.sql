SELECT * FROM order_items WHERE order_id IN (4, 15, 19, 20) AND (order_id IN (6, 15, 26, 28) OR quantity IS NOT NULL) ORDER BY 3 ASC
