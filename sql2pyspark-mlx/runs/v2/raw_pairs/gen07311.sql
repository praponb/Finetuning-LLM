SELECT c.email, c.name, a.status, a.customer_id FROM customers c LEFT OUTER JOIN accounts a ON a.customer_id = c.id ORDER BY a.status
