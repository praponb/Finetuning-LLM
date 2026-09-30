WITH prepared AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('MONTH', event_at) AS event_at_month
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per event_type
    SELECT *, LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id) AS prev_event_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR (UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(prev_event_at)) / 60 > 60 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
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
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM per_session
    WHERE duration_min > (SELECT AVG(duration_min) FROM per_session)
)
SELECT
    events,
    session_no,
    event_type,
    GROUPING_ID() AS grouping_level,
    COUNT(*) FILTER (WHERE event_type <> 'escalated') AS n_event_type_escalated,
    ROUND(SUM(duration_min) / NULLIF(SUM(session_no), 0), 3) AS duration_min_to_session_no_ratio,
    ROUND(SUM(session_no) / NULLIF(SUM(duration_min), 0), 3) AS session_no_to_duration_min_ratio
FROM without_matches
GROUP BY GROUPING SETS ((events, session_no, event_type), (events), (session_no), ())
