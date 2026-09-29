WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, CAST(amount AS INT) AS amount_int
    FROM transactions
    WHERE merchant IS NOT NULL
      AND merchant IN ('Starbucks', 'Shell', 'Lazada')
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM filtered
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        amount_int,
        SUM(is_new_session) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
-- Aggregate with conditional metrics
SELECT
    merchant,
    COUNT(DISTINCT txn_type) AS distinct_txn_types,
    MAX(txn_time) AS last_txn_time,
    APPROX_PERCENTILE(amount_int, 0.5) AS amount_int_p50,
    PERCENTILE_APPROX(burst_id, ARRAY(0.25, 0.5, 0.75)) AS burst_id_quartiles
FROM sessions_transactions
GROUP BY merchant
