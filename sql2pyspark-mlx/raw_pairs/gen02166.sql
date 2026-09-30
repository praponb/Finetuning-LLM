SELECT SUM(quantity * unit_price) AS revenue FROM order_items WHERE order_id IN (4, 5, 10, 13) OR order_id IN (5, 11, 16)
