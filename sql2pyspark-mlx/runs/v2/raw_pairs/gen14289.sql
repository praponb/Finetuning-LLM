SELECT a.customer_id, c.email FROM accounts a LEFT JOIN customers c ON a.customer_id = c.id
