WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN merchant IN ('Starbucks', 'Grab') THEN 'Priority' ELSE 'Other' END AS merchant_group,
        COALESCE(merchant, 'DIRECT') AS merchant_clean
    FROM transactions
    WHERE txn_time >= TO_TIMESTAMP('2023-07-05 09:00:00')
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM base
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
-- Aggregate with conditional metrics
SELECT
    island_id,
    CASE WHEN amount >= 4200 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
    APPROX_PERCENTILE(session_start_flag, 0.95) AS session_start_flag_p95,
    SUM(session_start_flag) AS total_session_start_flag,
    SUM(CASE WHEN merchant = 'Starbucks' THEN session_start_flag ELSE 0 END) AS starbucks_session_start_flag,
    COUNT(*) FILTER (WHERE merchant = 'Amazon') AS amazon_rows,
    COUNT(CASE WHEN merchant = 'Shell' THEN 1 END) AS shell_count,
    STDDEV_SAMP(amount) AS amount_stddev
FROM sessionized
GROUP BY CUBE (island_id, CASE WHEN amount >= 4200 THEN 'HIGH' ELSE 'LOW' END);
