WITH filtered_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type IS NOT NULL
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM filtered_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        actor,
        session_no,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY actor, session_no
),
labeled AS (
    -- Derive helper columns
    SELECT
        actor,
        session_no,
        session_start,
        session_end,
        duration_min,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown,
        DAYOFWEEK(session_start) AS session_start_dayofweek,
        CASE WHEN actor IN ('agent', 'customer') THEN 'group_a' WHEN actor = 'system' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM session_summary
    WHERE actor = 'system'
)
-- Subtotals with CUBE
SELECT
    actor_group,
    session_no,
    GROUPING_ID() AS grouping_level,
    ARRAY_SORT(COLLECT_SET(actor)) AS actor_list,
    MAX(session_end) AS last_session_end
FROM labeled
GROUP BY CUBE(actor_group, session_no)
ORDER BY actor_group NULLS LAST, session_no NULLS LAST
