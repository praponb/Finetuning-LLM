SELECT a.id, a.opened_date, a.customer_id, a.status FROM accounts a WHERE EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.txn_type = 'FEE')
