SELECT a.status, a.customer_id, t.txn_type, t.merchant FROM accounts a LEFT JOIN transactions t ON t.account_id = a.id WHERE a.customer_id = 3 ORDER BY t.merchant, a.customer_id
