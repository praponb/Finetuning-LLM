WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(event_type), 1, 3) AS event_type_prefix
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        prev_event_at,
        starts_session,
        session_id,
        ROUND((UNIX_TIMESTAMP(prev_event_at) - UNIX_TIMESTAMP(event_at)) / 60, 1) AS minutes_event_at_to_prev_event_at,
        HOUR(prev_event_at) AS prev_event_at_hour,
        ABS(starts_session - 0) AS starts_session_distance
    FROM sessionized
    WHERE event_at < TIMESTAMP '2025-02-24 10:00:00'
)
SELECT *
FROM (
    SELECT starts_session, actor, prev_event_at_hour
    FROM derived
) src
PIVOT (
    SUM(prev_event_at_hour)
    FOR actor IN ('agent', 'system', 'customer')
)
ORDER BY starts_session
