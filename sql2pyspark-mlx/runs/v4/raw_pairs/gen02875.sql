SELECT order_id, GREATEST(unit_price, quantity) AS greatest_unit_price_quantity FROM order_items WHERE quantity != 7 ORDER BY 2 DESC;
