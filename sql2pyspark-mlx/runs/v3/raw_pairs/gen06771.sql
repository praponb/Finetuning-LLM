SELECT o.customer_id, p.order_id, p.paid_at FROM orders o LEFT JOIN payments p ON p.order_id = o.id WHERE p.order_id IS NOT NULL OR o.amount >= 1480 ORDER BY order_id
