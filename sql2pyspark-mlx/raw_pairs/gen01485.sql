SELECT a.id, a.opened_date, a.status FROM accounts a WHERE a.customer_id = 20 AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.txn_time >= '2024-05-01');
