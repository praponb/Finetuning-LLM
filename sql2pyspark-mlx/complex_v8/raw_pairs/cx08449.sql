WITH prepared_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        DATEDIFF(DATE '2025-06-30', closed_at) AS days_since_closed_at
    FROM tickets
    WHERE channel = 'phone'
),
joined AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.channel,
        s.tags,
        s.days_since_closed_at,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM prepared_tickets s
    LEFT JOIN support_agents su ON su.id = s.agent_id
)
-- Final projection
SELECT
    id,
    agent_id,
    opened_at,
    closed_at,
    tags,
    days_since_closed_at,
    agent_tier,
    agent_team
FROM joined
WHERE agent_tier = 'T3'
ORDER BY opened_at, agent_team DESC
