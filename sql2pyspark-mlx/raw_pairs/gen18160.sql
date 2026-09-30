WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM transactions
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        SUM(is_new_session) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', txn_time) AS year_start,
    APPROX_PERCENTILE(session_no, ARRAY(0.25, 0.5, 0.75)) AS session_no_quartiles,
    COUNT_IF(txn_type = 'CREDIT') AS credit_events,
    MIN(amount) AS min_amount
FROM sessionized
GROUP BY DATE_TRUNC('YEAR', txn_time)
HAVING min_amount > 0
ORDER BY year_start DESC NULLS LAST;
