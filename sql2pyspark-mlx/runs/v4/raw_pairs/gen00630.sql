SELECT s.order_id, o.shipping_fee, o.amount FROM shipments s RIGHT JOIN orders o ON s.order_id = o.id WHERE s.order_id IN (28, 34) ORDER BY order_id DESC;
