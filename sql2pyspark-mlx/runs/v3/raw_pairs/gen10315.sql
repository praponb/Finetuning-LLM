SELECT s.cost, s.status, o.order_date, o.channel, o.shipping_fee FROM shipments s LEFT JOIN orders o ON o.id = s.order_id WHERE o.customer_id IS NULL AND s.status = 'LOST';
