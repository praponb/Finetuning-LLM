SELECT c.age, c.is_active, a.account_type, a.balance FROM customers c JOIN accounts a ON c.id = a.customer_id
