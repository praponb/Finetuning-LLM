SELECT p.id, p.amount, p.method, o.status, o.amount AS order_amount FROM payments p LEFT OUTER JOIN orders o ON p.order_id = o.id WHERE o.amount IS NOT NULL
