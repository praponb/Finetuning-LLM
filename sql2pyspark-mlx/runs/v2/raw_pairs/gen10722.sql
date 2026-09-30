SELECT
    id,
    account_id,
    txn_time,
    COALESCE(amount, 0) AS amount_filled,
    CASE WHEN txn_type = 'FEE' THEN 1 ELSE 0 END AS is_fee
FROM transactions
WHERE amount > 3200 AND (NOT is_flagged OR is_flagged)
ORDER BY 4 DESC
