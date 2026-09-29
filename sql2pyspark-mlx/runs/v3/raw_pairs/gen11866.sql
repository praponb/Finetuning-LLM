SELECT price FROM products UNION ALL SELECT unit_price AS price FROM order_items WHERE order_id IN (19, 31, 39)
