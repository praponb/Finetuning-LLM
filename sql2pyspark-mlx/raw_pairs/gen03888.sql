SELECT
    t.id,
    t.txn_time,
    t.merchant,
    t.txn_type
FROM transactions t
WHERE t.account_id IN (SELECT id FROM accounts WHERE account_type = 'CREDIT')
ORDER BY txn_type;
