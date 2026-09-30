SELECT o.shipping_fee, o.channel, o.customer_id, s.cost, s.order_id, s.status FROM orders o INNER JOIN shipments s ON o.id = s.order_id WHERE o.status != 'RETURNED'
