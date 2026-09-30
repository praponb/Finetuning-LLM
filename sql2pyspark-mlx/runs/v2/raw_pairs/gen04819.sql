SELECT o.order_date, o.customer_id, o.amount, c.email, c.is_active, c.credit_limit FROM orders o INNER JOIN customers c ON o.customer_id = c.id WHERE c.is_active ORDER BY amount DESC;
