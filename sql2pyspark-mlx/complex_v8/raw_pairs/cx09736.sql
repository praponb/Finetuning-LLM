WITH covered AS (
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
with_activity AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM covered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.csat > 3
        )
)
SELECT
    id,
    ticket_id,
    priority,
    agent_id,
    hours_since_open,
    FLOOR(hours_since_open / 10) * 10 AS hours_since_open_bucket
FROM with_activity
WHERE event_type = 'assigned'
ORDER BY id, hours_since_open DESC
