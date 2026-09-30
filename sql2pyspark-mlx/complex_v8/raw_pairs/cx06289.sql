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
    -- Compare against the overall max
    SELECT *
    FROM joined_ranges
    WHERE hours_since_open < (SELECT MAX(hours_since_open) FROM joined_ranges)
)
SELECT DISTINCT hours_since_open, event_type, ticket_id
FROM filtered
ORDER BY event_type DESC, hours_since_open
