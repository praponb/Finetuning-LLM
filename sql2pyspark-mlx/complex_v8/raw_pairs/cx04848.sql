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
joined AS (
    -- Join support_agents
    SELECT s.*, su.team AS agent_team, su.tier AS agent_tier
    FROM joined_ranges s
    JOIN support_agents su ON su.id = s.agent_id
),
qualified AS (
    -- Keep rows whose agent matches
    SELECT s.*
    FROM joined s
    WHERE EXISTS (
            SELECT 1
            FROM support_agents su
            WHERE su.id = s.agent_id
              AND YEAR(su.hired_on) = 2025
        )
)
SELECT
    id,
    ticket_id,
    event_type,
    priority,
    agent_id,
    hours_since_open,
    agent_team
FROM qualified
ORDER BY event_type, agent_team DESC NULLS FIRST
