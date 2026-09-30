SELECT
    txn_type,
    amount_bucket,
    total_amount,
    pct_debit
FROM (
    SELECT
        txn_type,
        CASE WHEN amount >= 3400 THEN 'high' WHEN amount >= 800 THEN 'mid' ELSE 'low' END AS amount_bucket,
        SUM(amount) AS total_amount,
        ROUND((100 * SUM(CASE WHEN txn_type = 'DEBIT' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_debit
    FROM transactions
    WHERE account_id = 15
    GROUP BY txn_type, CASE WHEN amount >= 3400 THEN 'high' WHEN amount >= 800 THEN 'mid' ELSE 'low' END
) AS t
WHERE pct_debit >= 74
ORDER BY total_amount
