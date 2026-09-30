SELECT a.id, a.account_type
FROM accounts a
WHERE EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.txn_time >= '2023-05-01')
ORDER BY a.account_type;
