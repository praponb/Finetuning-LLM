SELECT txn_type, account_id
FROM transactions
WHERE txn_type = 'CREDIT'
UNION ALL
SELECT txn_type, account_id
FROM transactions
WHERE txn_time < '2023-04-02'
