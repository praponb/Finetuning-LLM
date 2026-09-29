WITH prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time ASC, id ASC) AS prev_event_time
    FROM web_events
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY session_id ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY event_type ORDER BY duration_sec DESC NULLS LAST) AS duration_sec_percent_rank,
        MAX(is_new_session) OVER (PARTITION BY event_type ORDER BY CAST(event_time AS BIGINT) ASC RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS is_new_session_max_90d
    FROM sessionized
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        session_id,
        burst_id,
        duration_sec_percent_rank,
        is_new_session_max_90d,
        SUM(duration_sec) OVER w_recent AS running_sum_duration_sec,
        AVG(duration_sec) OVER w_recent AS running_avg_duration_sec,
        MIN(duration_sec) OVER w_recent AS running_min_duration_sec,
        NTILE(4) OVER (PARTITION BY session_id ORDER BY is_new_session DESC, id ASC) AS is_new_session_bucket
    FROM enriched
    WINDOW w_recent AS (PARTITION BY event_type ORDER BY duration_sec_percent_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(burst_id, 1) AS burst_id_rounded,
        ROUND(is_new_session_max_90d * 0.9, 2) AS is_new_session_max_90d_scaled
    FROM scored
    WHERE duration_sec_percent_rank >= 0.24
)
-- Join a dimension and aggregate
SELECT
    p.browser,
    COUNT(*) AS rows_total,
    MIN(src.running_sum_duration_sec) AS min_running_sum_duration_sec,
    ROUND(AVG(src.is_new_session_bucket), 2) AS avg_is_new_session_bucket
FROM valid src
INNER JOIN sessions p ON src.session_id = p.id
GROUP BY p.browser;
