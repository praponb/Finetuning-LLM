SELECT
    t.id,
    t.txn_time,
    t.account_id,
    t.amount
FROM transactions t
WHERE t.account_id IN (SELECT id FROM accounts WHERE opened_date > '2023-06-01')
ORDER BY txn_time
