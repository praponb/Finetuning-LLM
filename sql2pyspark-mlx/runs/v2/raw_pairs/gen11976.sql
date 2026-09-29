SELECT o.customer_id, o.order_date, o.discount, s.shipped_date, s.carrier, s.delivered_date FROM orders o LEFT OUTER JOIN shipments s ON o.id = s.order_id WHERE o.channel = 'mobile';
