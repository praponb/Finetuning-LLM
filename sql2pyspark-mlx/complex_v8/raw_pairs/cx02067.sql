WITH prepared AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        actor,
        session_no,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY actor, session_no
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(events, 0) AS events_filled,
        COALESCE(session_no, 0) AS session_no_filled,
        DATE_FORMAT(session_start, 'MMM yyyy') AS session_start_label
    FROM per_session
)
SELECT
    session_no,
    COALESCE(actor, 'ALL') AS actor,
    events,
    GROUPING_ID() AS grouping_level,
    COUNT(DISTINCT actor) AS distinct_actors,
    COUNT(*) AS n_rows
FROM annotated
GROUP BY CUBE(session_no, actor, events)
ORDER BY session_no NULLS LAST, actor NULLS LAST, events NULLS LAST
