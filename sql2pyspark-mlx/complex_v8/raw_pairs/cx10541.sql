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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 1 DAYS)
),
deduped AS (
    -- Keep the earliest row per ticket_id, event_type
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        priority,
        agent_id,
        hours_since_open
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_id, event_type ORDER BY event_at, id) AS rn
        FROM in_window
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    ticket_id,
    event_at,
    priority,
    agent_id
FROM deduped
WHERE event_at < TIMESTAMP '2025-02-12 12:00:00'
ORDER BY agent_id DESC NULLS FIRST, id
LIMIT 20
