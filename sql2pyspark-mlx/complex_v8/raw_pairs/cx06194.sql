WITH scoped AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type <> 'replied'
),
with_prev AS (
    -- Previous event_at per ticket_id
    SELECT *, LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS prev_event_at
    FROM scoped
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    prev_event_at,
    new_session_flag,
    session_no,
    ROUND(new_session_flag / NULLIF(session_no, 0), 3) AS new_session_flag_per_session_no,
    CASE WHEN new_session_flag >= 1 THEN 'A' WHEN new_session_flag >= 0 THEN 'B' ELSE 'C' END AS new_session_flag_tier
FROM sessionized
ORDER BY session_no, prev_event_at DESC, id
LIMIT 5
