SELECT a.status, t.merchant, t.txn_type FROM accounts a JOIN transactions t ON t.account_id = a.id WHERE a.balance > 69000;
