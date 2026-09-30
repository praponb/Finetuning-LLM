SELECT id, quantity, unit_price, LEAD(unit_price, 2, 0) OVER (ORDER BY quantity, id) AS next_unit_price FROM order_items WHERE quantity BETWEEN 3 AND 7;
