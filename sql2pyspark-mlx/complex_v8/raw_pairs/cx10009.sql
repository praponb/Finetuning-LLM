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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 2 DAYS)
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        event_at,
        event_type,
        priority,
        agent_id,
        LENGTH(TRIM(priority)) AS priority_len
    FROM covered
),
qualified AS (
    -- Keep rows whose agent matches
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM support_agents su
            WHERE su.id = s.agent_id
              AND su.tier IS NOT NULL
        )
)
-- Final projection
SELECT
    id,
    priority,
    agent_id,
    priority_len,
    TO_DATE(event_at) AS event_at_date
FROM qualified
WHERE agent_id IS NOT NULL
ORDER BY id DESC
LIMIT 5
