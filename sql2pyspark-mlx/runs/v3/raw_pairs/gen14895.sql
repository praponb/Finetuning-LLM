SELECT o.channel, s.shipped_date, s.delivered_date, s.status FROM orders o LEFT JOIN shipments s ON s.order_id = o.id ORDER BY o.channel DESC
