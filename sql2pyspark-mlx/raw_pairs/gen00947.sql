SELECT transactions.id, transactions.txn_type
FROM transactions
WHERE transactions.account_id IN (SELECT id FROM accounts WHERE YEAR(opened_date) = 2023);
