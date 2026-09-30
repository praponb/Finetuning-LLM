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
    -- Keep rows whose agent matches
    SELECT s.*
    FROM matched s
    WHERE EXISTS (
            SELECT 1
            FROM support_agents su
            WHERE su.id = s.agent_id
              AND su.team NOT IN ('Tech', 'VIP')
              AND su.tier IS NOT NULL
        )
),
screened AS (
    -- Keep rows whose agent matches
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM support_agents su
            WHERE su.id = s.agent_id
              AND LENGTH(TRIM(su.agent_name)) >= 5
              AND YEAR(su.hired_on) = 2025
        )
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    priority,
    agent_id,
    hours_since_open,
    DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at
FROM screened
ORDER BY ticket_id DESC, event_type
