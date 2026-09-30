SELECT s.id, s.carrier, o.store_id, o.customer_id, o.shipping_fee FROM shipments s JOIN orders o ON o.id = s.order_id WHERE NOT (o.discount > 0.25 AND MONTH(o.order_date) IN (2, 5, 10));
