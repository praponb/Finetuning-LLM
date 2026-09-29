SELECT unit_price FROM order_items WHERE unit_price > 340 UNION ALL SELECT amount AS unit_price FROM orders ORDER BY unit_price
