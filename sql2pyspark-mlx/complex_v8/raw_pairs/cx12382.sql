WITH valid_events AS (
    -- Scope the ticket_events rows
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
    FROM valid_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        actor,
        burst_id,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY actor, burst_id
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM per_session
    WHERE n_events > (SELECT AVG(n_events) FROM per_session)
)
SELECT
    actor,
    burst_id,
    session_start,
    session_end,
    n_events,
    duration_min,
    CASE WHEN actor IN ('customer', 'system') THEN 'group_a' WHEN actor = 'agent' THEN 'group_b' ELSE 'other' END AS actor_group,
    ROUND((UNIX_TIMESTAMP(session_end) - UNIX_TIMESTAMP(session_start)) / 60, 1) AS minutes_session_start_to_session_end
FROM screened
WHERE HOUR(session_end) BETWEEN 9 AND 15
ORDER BY actor DESC
