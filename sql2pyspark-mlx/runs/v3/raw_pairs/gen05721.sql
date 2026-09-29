SELECT o.order_date, p.status, p.method, p.order_id FROM orders o LEFT OUTER JOIN payments p ON p.order_id = o.id WHERE p.paid_at < '2025-10-04' ORDER BY order_date ASC, status DESC
