WITH prepared AS (
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
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR (UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(prev_event_at)) / 60 > 45 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        event_type,
        session_no,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY event_type, session_no
),
summary AS (
    -- Summarise per group
    SELECT
        session_no,
        DATE_TRUNC('MONTH', session_end) AS session_end_month,
        DATE_TRUNC('MONTH', session_start) AS session_start_month,
        COUNT(DISTINCT event_type) AS distinct_event_types,
        MIN(events) AS min_events
    FROM per_session
    GROUP BY 1, 2, 3
    HAVING AVG(session_no) > 2
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        session_no,
        session_end_month,
        session_start_month,
        distinct_event_types,
        min_events,
        LAST_VALUE(session_no) IGNORE NULLS OVER (PARTITION BY TO_DATE(session_end_month) ORDER BY distinct_event_types, session_no, session_end_month, session_start_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_session_no,
        ROUND(CUME_DIST() OVER (PARTITION BY TO_DATE(session_end_month) ORDER BY distinct_event_types), 3) AS distinct_event_types_cume_dist,
        COUNT(*) OVER (PARTITION BY session_no) AS rows_in_session_no,
        ROUND(session_no - AVG(session_no) OVER w_session_no, 2) AS session_no_vs_session_no_avg
    FROM summary
    WINDOW w_session_no AS (PARTITION BY session_no)
)
SELECT session_no, session_end_month, metric_name, value
FROM (
    SELECT session_no, session_end_month, last_known_session_no, rows_in_session_no, min_events
    FROM windowed
) src
UNPIVOT (
    value FOR metric_name IN (last_known_session_no, rows_in_session_no, min_events)
) unp
