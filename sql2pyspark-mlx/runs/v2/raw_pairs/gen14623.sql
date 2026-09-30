SELECT oi.product_id, o.shipping_fee, o.order_date, o.amount FROM order_items oi INNER JOIN orders o ON oi.order_id = o.id WHERE oi.product_id IN (7, 37, 38, 40) AND oi.order_id IN (3, 9, 19, 35);
