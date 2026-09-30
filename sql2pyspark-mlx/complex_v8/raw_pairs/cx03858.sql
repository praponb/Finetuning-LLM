WITH filtered AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per event_type
    SELECT *, LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id) AS prev_event_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR (UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(prev_event_at)) / 60 > 45 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        event_type,
        session_id,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY event_type, session_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(session_id) OVER w_session_id, 2) AS session_id_avg_session_id,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', session_end) ORDER BY session_end ASC), 3) AS session_end_cume_dist,
        LEAD(duration_min, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', session_end) ORDER BY session_end ASC, event_type, session_id) AS next2_duration_min,
        LAST_VALUE(duration_min) OVER (PARTITION BY DATE_TRUNC('MONTH', session_end) ORDER BY session_end ASC, event_type, session_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_duration_min,
        COUNT(*) OVER (PARTITION BY session_id) AS rows_in_session_id
    FROM session_summary
    WINDOW w_session_id AS (PARTITION BY session_id)
),
per_group AS (
    -- Summarise per group
    SELECT
        event_type,
        CASE WHEN rows_in_session_id >= 14 THEN 'HIGH' WHEN rows_in_session_id >= 8 THEN 'MEDIUM' ELSE 'LOW' END AS rows_in_session_id_tier,
        ROUND(SUM(session_id_avg_session_id) / NULLIF(SUM(rows_in_session_id), 0), 3) AS session_id_avg_session_id_to_rows_in_session_id_,
        ROUND(SUM(events) / NULLIF(SUM(rows_in_session_id), 0), 3) AS events_to_rows_in_session_id_ratio
    FROM metrics
    GROUP BY event_type, CASE WHEN rows_in_session_id >= 14 THEN 'HIGH' WHEN rows_in_session_id >= 8 THEN 'MEDIUM' ELSE 'LOW' END
)
SELECT *
FROM (
    SELECT event_type, rows_in_session_id_tier, session_id_avg_session_id_to_rows_in_session_id_
    FROM per_group
) src
PIVOT (
    MAX(session_id_avg_session_id_to_rows_in_session_id_) AS max_v, SUM(session_id_avg_session_id_to_rows_in_session_id_) AS sum_v
    FOR rows_in_session_id_tier IN ('LOW', 'MEDIUM')
)
ORDER BY event_type
