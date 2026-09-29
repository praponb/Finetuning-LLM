SELECT o.amount, p.method, p.paid_at FROM orders o LEFT JOIN payments p ON o.id = p.order_id ORDER BY p.method
