SELECT t.id, t.txn_time, t.account_id
FROM transactions t
WHERE t.txn_time < '2024-09-01' AND t.account_id IN (SELECT id FROM accounts WHERE YEAR(opened_date) = 2025)
