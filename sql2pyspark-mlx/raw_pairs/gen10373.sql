WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device,
        browser,
        started_at,
        country,
        TRIM(browser) AS clean_browser
    FROM sessions
    WHERE started_at > DATE '2023-07-15'
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM filtered
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        browser,
        started_at,
        country,
        SUM(is_new_burst) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.web_event_duration_sec
    FROM sessions_sessions s
    LEFT JOIN LATERAL (
            SELECT c.duration_sec AS web_event_duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(burst_id) OVER (PARTITION BY country ORDER BY burst_id, id) AS next_burst_id,
        LAG(burst_id) OVER (PARTITION BY country ORDER BY burst_id, id) AS prev_burst_id,
        LAG(web_event_duration_sec) OVER (PARTITION BY country ORDER BY burst_id, id) AS prev_web_event_duration_sec,
        LEAD(web_event_duration_sec) OVER (PARTITION BY country ORDER BY burst_id, id) AS next_web_event_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY burst_id, id) AS row_seq
    FROM with_last
)
-- Ranking and gap features
SELECT
    id,
    country,
    web_event_duration_sec,
    prev_web_event_duration_sec,
    next_web_event_duration_sec,
    row_seq,
    LAG(prev_burst_id) OVER w_recent AS prev_prev_burst_id,
    LEAD(next_burst_id) OVER w_recent AS next_next_burst_id,
    ROW_NUMBER() OVER w_recent AS row_seq_2,
    FIRST_VALUE(next_web_event_duration_sec, TRUE) OVER w_recent_2 AS first_known_next_web_event_duration_sec
FROM with_windows
WHERE TO_DATE(started_at) < '2024-03-01'
WINDOW w_recent AS (PARTITION BY browser ORDER BY prev_web_event_duration_sec, id), w_recent_2 AS (PARTITION BY country ORDER BY next_burst_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
