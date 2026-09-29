SELECT t.txn_time, t.txn_type, t.amount, a.status FROM transactions t JOIN accounts a ON t.account_id = a.id WHERE a.customer_id IN (3, 22) OR t.amount < 2700;
