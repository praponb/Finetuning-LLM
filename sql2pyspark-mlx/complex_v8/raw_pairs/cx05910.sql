WITH prepared_events AS (
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
    FROM prepared_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(starts_session / NULLIF(session_no, 0), 3) AS starts_session_per_session_no,
        SUBSTRING(TRIM(actor), 1, 3) AS actor_prefix
    FROM sessionized
    WHERE session_no < 5
),
with_activity AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM derived s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_type = 'resolved'
              AND t.actor NOT IN ('system', 'agent')
        )
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    prev_event_at,
    starts_session,
    starts_session_per_session_no,
    actor_prefix,
    ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(prev_event_at)) / 60, 1) AS minutes_prev_event_at_to_event_at,
    CASE WHEN actor IN ('agent', 'customer') THEN 'group_a' WHEN actor = 'system' THEN 'group_b' ELSE 'other' END AS actor_group
FROM with_activity
ORDER BY starts_session, id DESC
