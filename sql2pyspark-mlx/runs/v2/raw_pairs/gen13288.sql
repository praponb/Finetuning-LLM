SELECT p.amount, o.store_id, o.customer_id, c.credit_limit, c.email FROM payments p JOIN orders o ON p.order_id = o.id JOIN customers c ON c.id = o.customer_id ORDER BY o.store_id DESC;
