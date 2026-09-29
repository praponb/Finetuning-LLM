SELECT id, txn_type, account_id
FROM transactions
WHERE amount <= 1400
UNION ALL
SELECT id, txn_type, account_id
FROM transactions
WHERE is_flagged = FALSE
ORDER BY id DESC
