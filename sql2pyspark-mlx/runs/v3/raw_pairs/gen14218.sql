SELECT
    t.id,
    t.txn_time,
    t.account_id,
    t.merchant
FROM transactions t
WHERE HOUR(t.txn_time) BETWEEN 9 AND 16 AND t.account_id NOT IN (SELECT id FROM accounts WHERE status = 'FROZEN')
ORDER BY id;
