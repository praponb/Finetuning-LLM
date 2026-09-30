SELECT p.amount, o.amount AS order_amount, o.channel, c.credit_limit FROM payments p INNER JOIN orders o ON p.order_id = o.id INNER JOIN customers c ON o.customer_id = c.id
