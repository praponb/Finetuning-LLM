SELECT a.id, a.status, a.account_type, a.balance FROM accounts a WHERE a.opened_date <= '2024-07-01' AND a.customer_id IN (SELECT id FROM customers WHERE credit_limit > 12600);
