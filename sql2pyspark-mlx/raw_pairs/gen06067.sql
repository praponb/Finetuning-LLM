SELECT
    t.id,
    t.txn_time,
    t.is_flagged,
    t.account_id
FROM transactions t
WHERE t.account_id IN (SELECT id FROM accounts WHERE balance < 51000)
