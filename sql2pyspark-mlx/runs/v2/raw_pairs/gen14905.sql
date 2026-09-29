SELECT a.id, a.opened_date, a.account_type, a.balance FROM accounts a WHERE a.customer_id IN (SELECT id FROM customers WHERE signup_date > '2024-01-15') ORDER BY a.opened_date DESC, balance
