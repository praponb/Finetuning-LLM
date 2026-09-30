SELECT
    t.id,
    t.account_id,
    t.is_flagged,
    t.txn_type
FROM transactions t
WHERE t.account_id NOT IN (SELECT id FROM accounts WHERE status IS NOT NULL);
