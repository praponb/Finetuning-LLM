SELECT a.customer_id, a.status, a.account_type, t.txn_type, t.txn_time, t.amount FROM accounts a LEFT OUTER JOIN transactions t ON a.id = t.account_id WHERE a.balance > 56000;
