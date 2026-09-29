SELECT p.amount, o.discount, o.status, o.customer_id FROM payments p INNER JOIN orders o ON p.order_id = o.id WHERE HOUR(p.paid_at) BETWEEN 11 AND 17
