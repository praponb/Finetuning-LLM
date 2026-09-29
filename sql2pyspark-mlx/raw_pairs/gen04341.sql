WITH latest AS (
    SELECT
        id,
        account_id,
        amount,
        txn_time
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time NULLS FIRST, id) AS rn
        FROM transactions
    ) t
    WHERE rn = 1
)
SELECT
    account_id,
    MAX(txn_time) AS ended_at,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS duration_min,
    APPROX_PERCENTILE(amount, 0.9) AS amount_p90,
    APPROX_PERCENTILE(amount, 0.95) AS amount_p95,
    MIN(txn_time) AS first_seen
FROM latest
GROUP BY account_id
ORDER BY account_id DESC;
