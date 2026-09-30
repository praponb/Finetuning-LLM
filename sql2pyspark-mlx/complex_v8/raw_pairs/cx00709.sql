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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 1 DAYS)
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        event_type,
        priority,
        agent_id,
        hours_since_open,
        COALESCE(hours_since_open, 0.0) AS hours_since_open_filled,
        ABS(hours_since_open - 25.2) AS hours_since_open_distance
    FROM matched
)
SELECT id, agent_id, hours_since_open_filled
FROM derived
WHERE priority = 'P2'
