SELECT oi.id, oi.unit_price, oi.product_id, oi.order_id FROM order_items oi WHERE oi.order_id IN (SELECT id FROM orders WHERE discount BETWEEN 0.12 AND 0.26) ORDER BY oi.id;
