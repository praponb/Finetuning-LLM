WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT_WS('-', country, browser) AS country_browser_key
    FROM sessions
    WHERE started_at > TIMESTAMP '2023-07-19 10:00:00'
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device,
        started_at,
        country_browser_key,
        DATEDIFF(CURRENT_DATE(), started_at) AS days_since_started_at
    FROM base
    WHERE started_at > TIMESTAMP '2024-02-12 21:00:00'
      AND device IN ('desktop', 'tablet', 'mobile')
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(days_since_started_at) OVER (PARTITION BY device ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS days_since_started_at_sum_90d,
        LEAD(days_since_started_at, 1) OVER w_grp AS next_days_since_started_at,
        COUNT(days_since_started_at) OVER (ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS days_since_started_at_count_7d,
        NTILE(5) OVER (PARTITION BY device ORDER BY days_since_started_at, id) AS days_since_started_at_bucket
    FROM valid
    WINDOW w_grp AS (PARTITION BY device ORDER BY started_at, id)
),
latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.event_type
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics_sessions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(days_since_started_at_sum_90d) OVER (PARTITION BY device ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS days_since_started_at_sum_90d_count_7d,
        LEAD(days_since_started_at_count_7d, 1) OVER (PARTITION BY device ORDER BY days_since_started_at_count_7d, id) AS next_days_since_started_at_count_7d,
        SUM(days_since_started_at_sum_90d) OVER (ORDER BY next_days_since_started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_since_started_at_sum_90d
    FROM latest_match
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(started_at) OVER (PARTITION BY event_type ORDER BY started_at, id) AS prev_started_at
    FROM metrics_sessions
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY event_type ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY days_since_started_at_sum_90d DESC) AS days_since_started_at_sum_90d_dense_rank,
        MAX(running_days_since_started_at_sum_90d) OVER (PARTITION BY event_type ORDER BY CAST(prev_started_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS running_days_since_started_at_sum_90d_max_7d
    FROM sessions_sessions
)
SELECT
    id,
    device,
    country_browser_key,
    days_since_started_at,
    days_since_started_at_sum_90d,
    next_days_since_started_at,
    days_since_started_at_count_7d,
    days_since_started_at_bucket,
    days_since_started_at_sum_90d_count_7d,
    CASE WHEN device RLIKE '^(tablet|desktop)$' THEN 'EXTENDED' WHEN device RLIKE '^(mobile)$' THEN 'CORE' ELSE 'GROUP_A' END AS device_zone
FROM with_windows
ORDER BY device_zone DESC NULLS LAST;
