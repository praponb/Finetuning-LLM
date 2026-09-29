WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, INITCAP(TRIM(txn_type)) AS txn_type_title
    FROM transactions
    WHERE merchant IS NOT NULL
      AND amount < 1900
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM valid
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        txn_type_title,
        is_new_burst,
        burst_id,
        DATEDIFF(CURRENT_DATE(), txn_time) AS days_since_txn_time
    FROM sessionized
    WHERE txn_time >= DATE '2023-07-28'
      AND merchant IS DISTINCT FROM 'Lazada'
),
with_gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM filtered
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps_2
),
filtered_transactions AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        txn_type,
        amount,
        txn_time,
        is_flagged,
        txn_type_title,
        days_since_txn_time,
        session_start_flag,
        session_no,
        CAST(days_since_txn_time AS STRING) AS days_since_txn_time_num
    FROM sessions_transactions
    WHERE is_flagged
      AND merchant IS NOT NULL
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM filtered_transactions
),
sessionized_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no_2
    FROM gaps
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN session_start_flag >= 1 THEN 'HIGH' ELSE 'LOW' END AS session_start_flag_tier,
    CASE WHEN days_since_txn_time >= 87 THEN 'HIGH' ELSE 'LOW' END AS days_since_txn_time_tier,
    COUNT(*) FILTER (WHERE txn_type = 'REFUND') AS refund_rows,
    MIN(session_start_flag) AS min_session_start_flag,
    ROUND(AVG(session_start_flag), 2) AS avg_session_start_flag,
    SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list,
    COUNT_IF(txn_type = 'DEBIT') AS debit_events,
    AVG(amount) FILTER (WHERE txn_type = 'REFUND') AS refund_avg_amount
FROM sessionized_transactions
GROUP BY CASE WHEN session_start_flag >= 1 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN days_since_txn_time >= 87 THEN 'HIGH' ELSE 'LOW' END
HAVING min_session_start_flag > 0
ORDER BY days_since_txn_time_tier DESC;
