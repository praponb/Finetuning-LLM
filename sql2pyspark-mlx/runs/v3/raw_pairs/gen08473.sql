SELECT unit_price FROM order_items UNION ALL SELECT amount FROM transactions ORDER BY unit_price DESC;
