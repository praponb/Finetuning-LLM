SELECT a.balance, t.txn_type, t.amount FROM accounts a LEFT JOIN transactions t ON t.account_id = a.id WHERE a.status = 'FROZEN' AND t.amount <= 3700;
