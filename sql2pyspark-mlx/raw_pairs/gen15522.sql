SELECT
    id,
    txn_type,
    amount,
    txn_time,
    is_flagged,
    SUM(amount) OVER (PARTITION BY merchant ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS amount_sum_90d,
    ROUND(amount / SUM(amount) OVER (PARTITION BY account_id), 4) AS amount_share,
    NTILE(5) OVER w_running AS amount_bucket
FROM transactions
WHERE txn_time >= '2023-07-17'
WINDOW w_running AS (ORDER BY amount, id)
