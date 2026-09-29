SELECT o.id, o.customer_id, o.store_id, o.amount FROM orders o WHERE o.channel = 'mobile' AND o.id IN (SELECT order_id FROM shipments WHERE MONTH(delivered_date) IN (1, 2, 3)) ORDER BY o.id DESC
