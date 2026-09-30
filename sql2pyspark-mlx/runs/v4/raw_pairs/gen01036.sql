SELECT a.balance, t.txn_time
FROM accounts a
LEFT JOIN transactions t ON t.account_id = a.id;
