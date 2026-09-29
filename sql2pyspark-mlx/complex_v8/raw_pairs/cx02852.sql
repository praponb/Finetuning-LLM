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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 2 DAYS)
),
filtered AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM matched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.priority IN ('P2', 'P4', 'P1')
              AND t.csat >= 3
        )
),
with_parent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.agent_id,
        s.hours_since_open,
        su.team AS agent_team,
        su.agent_name,
        su.tier AS agent_tier,
        t.tags AS ticket_tags
    FROM filtered s
    LEFT JOIN support_agents su ON su.id = s.agent_id
    LEFT JOIN tickets t ON t.id = s.ticket_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    hours_since_open,
    agent_name,
    agent_tier,
    ticket_tags
FROM with_parent
WHERE agent_tier = 'T1'
ORDER BY ticket_id NULLS LAST, id
LIMIT 5
