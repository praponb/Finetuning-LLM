SELECT o.order_date, c.credit_limit FROM orders o LEFT JOIN customers c ON o.customer_id = c.id;
