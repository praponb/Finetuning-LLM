WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        NTILE(3) OVER w_part AS amount_bucket,
        ROUND(amount / SUM(amount) OVER (PARTITION BY txn_type), 4) AS amount_share,
        LAST_VALUE(merchant) IGNORE NULLS OVER w_part_2 AS last_known_merchant,
        SUM(amount) OVER w_part_2 AS cumulative_amount,
        NTILE(3) OVER (PARTITION BY merchant ORDER BY amount DESC, id ASC) AS amount_bucket_2
    FROM transactions
    WHERE is_flagged
    WINDOW w_part AS (PARTITION BY account_id ORDER BY amount DESC, id ASC), w_part_2 AS (PARTITION BY account_id ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time ASC, id ASC) AS prev_txn_time
    FROM with_windows
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY merchant ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM sessionized
),
sessionized_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
with_gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time ASC, id ASC) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time ASC, id ASC) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM sessionized_transactions
),
sessionized_transactions_2 AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        txn_type,
        txn_time,
        merchant,
        amount_bucket,
        cumulative_amount,
        amount_bucket_2,
        prev_txn_time,
        SUM(session_start_flag) OVER (PARTITION BY merchant ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no_2
    FROM with_gaps_2
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM sessionized_transactions_2
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        txn_type,
        txn_time,
        amount_bucket,
        amount_bucket_2,
        prev_txn_time,
        SUM(is_new_burst) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        txn_type,
        txn_time,
        amount_bucket,
        amount_bucket_2,
        MIN(amount_bucket) OVER w_hist AS last4_min_amount_bucket,
        COUNT(amount_bucket) OVER w_hist AS last4_count_amount_bucket,
        AVG(amount_bucket) OVER w_hist AS last4_avg_amount_bucket,
        SUM(amount_bucket_2) OVER (ORDER BY CAST(txn_time AS BIGINT) ASC RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS amount_bucket_2_sum_90d
    FROM sessions_transactions
    WINDOW w_hist AS (PARTITION BY txn_type ORDER BY amount_bucket_2 ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN amount_bucket_2_sum_90d >= 38 THEN 'HIGH' ELSE 'LOW' END AS amount_bucket_2_sum_90d_tier,
    CASE WHEN amount_bucket_2 >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_bucket_2_tier,
    txn_type,
    SIZE(COLLECT_SET(txn_type)) AS num_txn_types,
    MAX(last4_avg_amount_bucket) AS max_last4_avg_amount_bucket
FROM enriched
GROUP BY CASE WHEN amount_bucket_2_sum_90d >= 38 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN amount_bucket_2 >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END, txn_type
ORDER BY max_last4_avg_amount_bucket ASC NULLS LAST
