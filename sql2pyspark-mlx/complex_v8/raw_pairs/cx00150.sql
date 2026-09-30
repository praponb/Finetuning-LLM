WITH filtered_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat
    FROM tickets
    WHERE closed_at < TIMESTAMP '2025-03-04 11:00:00'
),
joined_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.channel,
        s.csat,
        su.team AS agent_team
    FROM filtered_tickets s
    JOIN support_agents su ON su.id = s.agent_id AND su.team IN ('Onboarding', 'Tech')
    WHERE su.team IS NOT NULL
),
with_details_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.channel,
        s.csat,
        s.agent_team,
        su.team AS agent_team_2
    FROM joined_agent s
    JOIN support_agents su ON su.id = s.agent_id
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_details_agent
    WHERE csat > (SELECT AVG(csat) FROM with_details_agent)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT agent_team, agent_team_2, csat
    FROM without_matches
) src
PIVOT (
    COUNT(csat)
    FOR agent_team_2 IN ('Tech', 'VIP', 'Billing')
)
