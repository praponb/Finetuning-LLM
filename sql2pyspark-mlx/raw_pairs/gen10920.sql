SELECT o.channel, COUNT(*) AS cnt, MAX(oi.quantity) - MIN(oi.quantity) AS quantity_range FROM order_items oi LEFT OUTER JOIN orders o ON oi.order_id = o.id GROUP BY o.channel;
