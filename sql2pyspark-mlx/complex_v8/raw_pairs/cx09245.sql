WITH scoped AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        tags
    FROM tickets
),
grouped AS (
    -- Group-level totals
    SELECT
        agent_id,
        TO_DATE(closed_at) AS closed_at_day,
        DATE_FORMAT(opened_at, 'yyyy-MM') AS opened_at_ym,
        SUM(requester_id) AS total_requester_id,
        COUNT(*) AS cnt
    FROM scoped
    GROUP BY agent_id, TO_DATE(closed_at), DATE_FORMAT(opened_at, 'yyyy-MM')
),
joined_agent AS (
    -- Join support_agents
    SELECT
        s.agent_id,
        s.closed_at_day,
        s.opened_at_ym,
        s.cnt,
        su.tier AS agent_tier
    FROM grouped s
    JOIN support_agents su ON su.id = s.agent_id
),
exploded_detail_tickets AS (
    -- One row per ticket
    SELECT
        s.agent_id,
        s.closed_at_day,
        s.opened_at_ym,
        s.agent_tier,
        t.id AS ticket_id,
        t.csat AS ticket_csat,
        t.closed_at AS ticket_closed_at,
        t.requester_id AS ticket_requester_id,
        t.tags AS ticket_tags
    FROM joined_agent s
    JOIN tickets t ON t.agent_id = s.agent_id
)
SELECT
    agent_id,
    closed_at_day,
    opened_at_ym,
    agent_tier,
    ticket_id,
    CONCAT(agent_tier, '#', CAST(ticket_csat AS STRING)) AS agent_tier_ref
FROM exploded_detail_tickets
WHERE ticket_requester_id <= 18
ORDER BY opened_at_ym DESC NULLS FIRST, agent_id
