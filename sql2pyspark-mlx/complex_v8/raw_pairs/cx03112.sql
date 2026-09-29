WITH in_window AS (
    -- Events that happened while the ticket was open
    SELECT
        e.id,
        e.ticket_id,
        e.event_at,
        e.event_type,
        t.priority,
        t.agent_id,
        ROUND((UNIX_TIMESTAMP(e.event_at) - UNIX_TIMESTAMP(t.opened_at)) / 3600, 2) AS hours_since_open
    FROM ticket_events e
    JOIN tickets t
      ON t.id = e.ticket_id
     AND e.event_at >= t.opened_at
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 7 DAYS)
),
annotated AS (
    -- Derive helper columns
    SELECT *, LOWER(TRIM(priority)) AS priority_clean
    FROM in_window
)
-- Final projection
SELECT
    id,
    event_type,
    priority,
    agent_id,
    hours_since_open,
    priority_clean
FROM annotated
ORDER BY priority_clean DESC
