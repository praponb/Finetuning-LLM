SELECT
    t.id,
    t.is_flagged,
    t.amount,
    t.txn_type
FROM transactions t
WHERE t.account_id = 28 AND t.account_id NOT IN (SELECT id FROM accounts WHERE customer_id IS NULL);
