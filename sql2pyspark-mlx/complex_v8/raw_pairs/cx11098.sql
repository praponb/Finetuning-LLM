WITH filtered_agents AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        CONCAT(team, '#', CAST(id AS STRING)) AS team_ref,
        CONCAT(tier, '#', CAST(id AS STRING)) AS tier_ref
    FROM support_agents
    WHERE hired_on BETWEEN DATE '2024-07-09' AND DATE '2025-02-26'
      AND team IS NOT NULL
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        agent_name,
        tier,
        hired_on,
        team_ref,
        tier_ref,
        CASE WHEN team IN ('Billing', 'Tech') THEN 'group_a' WHEN team = 'VIP' THEN 'group_b' ELSE 'other' END AS team_group
    FROM filtered_agents
),
with_children AS (
    -- One row per ticket
    SELECT
        s.id,
        s.agent_name,
        s.tier,
        s.tier_ref,
        s.team_group,
        t.id AS ticket_id,
        t.tags AS ticket_tags,
        t.opened_at AS ticket_opened_at,
        t.requester_id AS ticket_requester_id
    FROM scored s
    JOIN tickets t ON t.agent_id = s.id
)
SELECT *
FROM (
    SELECT ticket_opened_at, team_group, ticket_requester_id
    FROM with_children
) src
PIVOT (
    COUNT(ticket_requester_id)
    FOR team_group IN ('other', 'group_a')
)
ORDER BY ticket_opened_at
