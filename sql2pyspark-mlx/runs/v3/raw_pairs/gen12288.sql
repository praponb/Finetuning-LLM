SELECT unit_price FROM order_items WHERE product_id IN (18, 25, 28) UNION SELECT shipping_fee AS unit_price FROM orders;
