SELECT a.id, a.status, a.customer_id, a.balance FROM accounts a WHERE a.customer_id NOT IN (SELECT id FROM customers WHERE is_active);
