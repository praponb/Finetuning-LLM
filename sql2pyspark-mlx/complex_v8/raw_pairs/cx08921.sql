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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 3 DAYS)
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM covered
    WHERE hours_since_open > (SELECT MIN(hours_since_open) FROM covered)
)
SELECT
    id,
    ticket_id,
    event_type,
    agent_id,
    hours_since_open,
    FLOOR(hours_since_open / 10) * 10 AS hours_since_open_bucket
FROM filtered
ORDER BY ticket_id DESC NULLS FIRST, agent_id
