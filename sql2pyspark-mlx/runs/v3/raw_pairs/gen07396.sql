SELECT
    id,
    account_id,
    merchant,
    COALESCE(amount, 0) AS amount_filled,
    TRIM(txn_type) AS clean_txn_type
FROM transactions
WHERE is_flagged = TRUE OR txn_type = 'CREDIT';
