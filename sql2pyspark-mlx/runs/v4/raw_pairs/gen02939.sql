SELECT
    id,
    merchant,
    is_flagged,
    txn_time,
    COALESCE(amount, 0) AS amount_filled
FROM transactions
WHERE (amount >= 1700 AND TO_DATE(txn_time) < '2024-10-01') OR account_id IN (29, 31, 33, 39);
