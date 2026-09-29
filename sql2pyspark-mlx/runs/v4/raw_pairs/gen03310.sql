SELECT unit_price, 'order_item' AS source FROM order_items UNION SELECT amount AS unit_price, 'payment' AS source FROM payments ORDER BY unit_price;
