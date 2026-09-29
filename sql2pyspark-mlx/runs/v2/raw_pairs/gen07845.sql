SELECT t.id, t.txn_time
FROM transactions t
WHERE t.account_id IN (SELECT id FROM accounts WHERE balance >= 19000)
ORDER BY txn_time, id
LIMIT 10;
