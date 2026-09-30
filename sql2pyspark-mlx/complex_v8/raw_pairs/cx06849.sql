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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 1 DAYS)
),
screened AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM covered s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.csat > 3
              AND t.requester_id > 9
        )
)
-- Final projection
SELECT
    id,
    ticket_id,
    priority,
    agent_id,
    DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at,
    CONCAT(event_type, '#', CAST(id AS STRING)) AS event_type_ref
FROM screened
WHERE agent_id IS NOT NULL
ORDER BY id DESC, event_type_ref DESC
