SELECT e.*, o.channel, o.status, o.discount FROM order_items e LEFT JOIN orders o ON e.order_id = o.id;
