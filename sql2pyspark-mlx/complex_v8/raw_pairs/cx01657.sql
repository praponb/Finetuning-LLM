WITH recent_agents AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_name,
        team,
        tier,
        CASE WHEN team IN ('Billing', 'Tech') THEN 'group_a' WHEN team = 'Onboarding' THEN 'group_b' ELSE 'other' END AS team_group
    FROM support_agents
    WHERE tier IN ('T1', 'T2')
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT SUM(t.csat) FROM tickets t WHERE t.agent_id = s.id) AS total_ticket_csat,
        (SELECT MAX(t.csat) FROM tickets t WHERE t.agent_id = s.id) AS max_ticket_csat
    FROM recent_agents s
),
line_level_tickets AS (
    -- One row per ticket
    SELECT
        s.id,
        s.agent_name,
        s.tier,
        s.team_group,
        s.total_ticket_csat,
        s.max_ticket_csat,
        t.id AS ticket_id,
        t.csat AS ticket_csat,
        t.tags AS ticket_tags
    FROM with_stats s
    JOIN tickets t ON t.agent_id = s.id
)
SELECT
    id,
    tier,
    team_group,
    ticket_id,
    ticket_tags,
    COALESCE(team_group, 'UNKNOWN') AS team_group_or_unknown,
    UPPER(TRIM(agent_name)) AS agent_name_clean
FROM line_level_tickets
WHERE SIZE(ticket_tags) >= 2
ORDER BY tier DESC
