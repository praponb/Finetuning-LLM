SELECT c.email, a.status, a.balance, a.opened_date FROM customers c JOIN accounts a ON a.customer_id = c.id WHERE c.is_active = TRUE ORDER BY a.status DESC
