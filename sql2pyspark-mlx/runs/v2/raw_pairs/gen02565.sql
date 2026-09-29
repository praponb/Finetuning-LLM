SELECT p.paid_at, p.method, o.amount FROM payments p FULL OUTER JOIN orders o ON p.order_id = o.id
