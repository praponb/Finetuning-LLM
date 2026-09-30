SELECT t.amount, t.txn_type, a.customer_id FROM transactions t INNER JOIN accounts a ON a.id = t.account_id
