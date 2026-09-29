WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        k,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS k, value
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN created_at - LAG(created_at) OVER (PARTITION BY k ORDER BY created_at, id, k) > INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS session_start_flag
    FROM lines
),
sessions_carts AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        created_at,
        k,
        value,
        SUM(session_start_flag) OVER (PARTITION BY k ORDER BY created_at, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        SUBSTRING(k, 1, 2) AS k_prefix,
        ROUND(session_id * 1.1, 2) AS session_id_scaled,
        COALESCE(k, 'NONE') AS k_clean
    FROM sessions_carts
    WHERE created_at > TIMESTAMP '2025-03-01 16:00:00'
      AND k IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY k ORDER BY session_id_scaled, id, k) AS session_id_scaled_bucket,
        LAG(session_id_scaled, 1) OVER w_hist AS prev_session_id_scaled,
        ROW_NUMBER() OVER w_hist_2 AS row_seq
    FROM valid
    WINDOW w_hist AS (PARTITION BY DATE_TRUNC('YEAR', created_at), k ORDER BY created_at, id, k), w_hist_2 AS (ORDER BY created_at DESC, id, k)
)
-- Grouped metrics
SELECT
    CASE WHEN session_id >= 8 THEN 'LARGE' ELSE 'SMALL' END AS session_id_tier,
    session_id,
    ROUND(SUM(prev_session_id_scaled) / NULLIF(COUNT(CASE WHEN k = 'utm_source' THEN 1 END), 0), 2) AS prev_session_id_scaled_per_utm_source,
    MAX(created_at) AS last_seen,
    ROUND(SUM(row_seq) / NULLIF(COUNT(CASE WHEN k = 'device' THEN 1 END), 0), 2) AS row_seq_per_device,
    COUNT(CASE WHEN k = 'device' THEN 1 END) AS device_count,
    SUM(CASE WHEN k = 'device' THEN prev_session_id_scaled ELSE 0 END) AS device_prev_session_id_scaled,
    ROUND(SUM(row_seq) / NULLIF(COUNT(CASE WHEN k = 'weight_kg' THEN 1 END), 0), 2) AS row_seq_per_weight_kg
FROM enriched
GROUP BY CASE WHEN session_id >= 8 THEN 'LARGE' ELSE 'SMALL' END, session_id
HAVING device_prev_session_id_scaled > 0
ORDER BY device_prev_session_id_scaled
