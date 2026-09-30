SELECT o.status, oi.product_id FROM orders o LEFT OUTER JOIN order_items oi ON oi.order_id = o.id WHERE o.shipping_fee <= 33 AND o.amount <= 1160 ORDER BY 2 DESC
