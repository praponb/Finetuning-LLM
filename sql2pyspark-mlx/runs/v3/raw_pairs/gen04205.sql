SELECT s.status, s.carrier, s.cost, o.discount, o.order_date, o.shipping_fee FROM shipments s JOIN orders o ON s.order_id = o.id WHERE o.status = 'PENDING' ORDER BY order_date;
