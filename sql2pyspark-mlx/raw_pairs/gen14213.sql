SELECT
    id,
    amount,
    is_flagged,
    txn_type,
    ROUND(amount * 0.9, 2) AS amount_scaled,
    DATEDIFF(CURRENT_DATE(), txn_time) / 365 AS years_since_txn_time
FROM transactions
