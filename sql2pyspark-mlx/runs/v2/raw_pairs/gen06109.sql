SELECT a.id, a.customer_id, a.opened_date, a.status FROM accounts a WHERE NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.txn_time < '2023-08-10') ORDER BY status DESC;
