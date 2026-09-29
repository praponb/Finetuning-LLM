SELECT a.customer_id, a.status, a.balance, t.txn_type, t.is_flagged FROM accounts a LEFT OUTER JOIN transactions t ON a.id = t.account_id ORDER BY txn_type ASC;
