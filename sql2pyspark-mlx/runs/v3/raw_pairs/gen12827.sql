SELECT a.id, a.account_type, a.status, a.balance FROM accounts a WHERE a.customer_id IN (SELECT id FROM customers WHERE is_active)
