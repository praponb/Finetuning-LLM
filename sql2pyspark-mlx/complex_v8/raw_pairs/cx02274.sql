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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 3 DAYS)
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM joined_ranges
    WHERE hours_since_open > (SELECT AVG(hours_since_open) FROM joined_ranges)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    priority,
    agent_id,
    hours_since_open
FROM with_activity
WHERE ticket_id IS NOT NULL
ORDER BY hours_since_open DESC
