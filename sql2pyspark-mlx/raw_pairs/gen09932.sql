SELECT o.shipping_fee, s.cost, s.shipped_date, s.carrier FROM orders o LEFT OUTER JOIN shipments s ON o.id = s.order_id WHERE o.customer_id IN (19, 27) AND o.shipping_fee >= 26;
