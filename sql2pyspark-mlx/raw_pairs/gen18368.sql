SELECT s.id, s.delivered_date, s.carrier, s.order_id, o.order_date, o.shipping_fee FROM shipments s INNER JOIN orders o ON s.order_id = o.id WHERE s.carrier = 'Kerry' AND o.discount > 0.28;
