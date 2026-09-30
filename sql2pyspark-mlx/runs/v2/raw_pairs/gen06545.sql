SELECT
    id,
    amount,
    is_flagged,
    txn_type
FROM transactions
WHERE amount >= (SELECT AVG(amount) FROM transactions WHERE account_id = 24)
ORDER BY txn_type
