SELECT a.id, a.balance, a.customer_id, a.opened_date FROM accounts a WHERE NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.txn_time < '2024-07-01')
