SELECT MAX(quantity) AS max_quantity, SUM(unit_price * quantity) AS revenue FROM order_items ORDER BY max_quantity DESC
