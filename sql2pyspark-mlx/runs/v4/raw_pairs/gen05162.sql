SELECT
    id,
    txn_type,
    is_flagged,
    COALESCE(amount, 0) AS amount_filled
FROM transactions
