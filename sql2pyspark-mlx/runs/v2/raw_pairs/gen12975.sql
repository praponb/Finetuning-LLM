SELECT transactions.id, transactions.txn_time
FROM transactions
WHERE transactions.account_id IN (SELECT id FROM accounts WHERE balance < 42000);
