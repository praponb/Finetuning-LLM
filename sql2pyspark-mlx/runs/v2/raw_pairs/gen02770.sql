SELECT id, order_id, quantity, unit_price, SUM(unit_price) OVER (PARTITION BY order_id ORDER BY quantity, id) AS running_unit_price FROM order_items WHERE quantity > 3;
