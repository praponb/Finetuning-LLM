WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_TRUNC('week', txn_time) AS txn_time_period,
        CONCAT_WS('-', txn_type, merchant) AS txn_type_merchant_key,
        COALESCE(merchant, 'OTHER') AS merchant_filled
    FROM transactions
    WHERE merchant IS NOT NULL
      AND merchant IS DISTINCT FROM 'Shell'
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time_period - LAG(txn_time_period) OVER (PARTITION BY account_id ORDER BY txn_time_period, id) > INTERVAL '7' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM base
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY account_id ORDER BY txn_time_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
flagged_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time_period - LAG(txn_time_period) OVER (PARTITION BY txn_type ORDER BY txn_time_period, id) > INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM sessionized
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        txn_time_period,
        txn_type_merchant_key,
        session_start_flag,
        island_id,
        SUM(is_new_session) OVER (PARTITION BY txn_type ORDER BY txn_time_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id_2
    FROM flagged_2
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(island_id) OVER w_hist AS next_island_id,
        LAG(session_start_flag) OVER w_hist AS prev_session_start_flag,
        LEAD(session_start_flag) OVER w_hist AS next_session_start_flag,
        ROW_NUMBER() OVER w_hist AS event_seq
    FROM islands
    WHERE island_id_2 BETWEEN 2 AND 5
    WINDOW w_hist AS (PARTITION BY txn_type ORDER BY txn_time, id)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        txn_type,
        is_flagged,
        txn_time_period,
        txn_type_merchant_key,
        island_id,
        next_island_id,
        prev_session_start_flag,
        next_session_start_flag
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY next_island_id, id) = 1
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', txn_time_period) AS quarter_start,
    SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list,
    ROUND(AVG(next_island_id), 2) AS avg_next_island_id
FROM latest
GROUP BY 1
HAVING ROUND(AVG(next_island_id), 2) >= 0
ORDER BY avg_next_island_id DESC;
