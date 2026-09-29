SELECT a.id, a.customer_id, a.balance, a.status FROM accounts a WHERE a.opened_date BETWEEN '2024-08-01' AND '2025-02-01' AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id)
