WITH joined_ranges AS (
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
    FROM joined_ranges s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND NOT ARRAY_CONTAINS(t.tags, 'bug')
              AND t.opened_at >= TIMESTAMP '2025-01-19 07:00:00'
        )
      AND HOUR(s.event_at) BETWEEN 8 AND 13
),
with_parent_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.priority,
        s.agent_id,
        s.hours_since_open,
        t.priority AS ticket_priority
    FROM filtered s
    JOIN tickets t ON t.id = s.ticket_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    priority,
    agent_id,
    hours_since_open,
    ticket_priority
FROM with_parent_ticket
WHERE hours_since_open <= 50.4
ORDER BY hours_since_open NULLS LAST, id DESC NULLS FIRST
