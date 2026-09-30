WITH matched AS (
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
filtered AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM matched s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND ARRAY_CONTAINS(t.tags, 'outage')
        )
),
with_details_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.priority,
        s.agent_id,
        s.hours_since_open,
        t.priority AS ticket_priority
    FROM filtered s
    JOIN tickets t ON t.id = s.ticket_id
),
derived AS (
    -- Add calculated columns
    SELECT *, CONCAT_WS(' / ', event_type, priority) AS event_type_priority_label
    FROM with_details_ticket
    WHERE ticket_id IS NOT NULL
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    priority,
    agent_id,
    ticket_priority,
    event_type_priority_label
FROM derived
ORDER BY ticket_id, id
LIMIT 5
