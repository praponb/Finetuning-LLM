SELECT a.id, a.customer_id, a.opened_date, a.balance FROM accounts a WHERE a.opened_date > '2025-09-01' AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id);
