SELECT a.customer_id, a.balance, a.account_type, t.txn_time FROM accounts a LEFT JOIN transactions t ON a.id = t.account_id WHERE a.status != 'CLOSED';
