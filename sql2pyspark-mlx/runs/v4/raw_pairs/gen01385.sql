SELECT s.cost, s.order_id, o.customer_id, o.store_id FROM shipments s RIGHT JOIN orders o ON s.order_id = o.id WHERE MONTH(o.order_date) IN (1, 2, 8) AND o.shipping_fee < 40;
