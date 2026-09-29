WITH base_agents AS (
    -- Filter support_agents
    SELECT
        id,
        agent_name,
        team,
        tier,
        CONCAT(team, '#', CAST(id AS STRING)) AS team_ref,
        QUARTER(hired_on) AS hired_on_quarter
    FROM support_agents
    WHERE team = 'Onboarding'
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(hired_on_quarter - 2) AS hired_on_quarter_distance,
        LOWER(TRIM(agent_name)) AS agent_name_clean
    FROM base_agents
),
with_children AS (
    -- One row per ticket
    SELECT
        s.id,
        s.agent_name,
        s.team,
        s.tier,
        s.hired_on_quarter,
        s.agent_name_clean,
        t.id AS ticket_id,
        t.closed_at AS ticket_closed_at,
        t.requester_id AS ticket_requester_id,
        t.priority AS ticket_priority,
        t.channel AS ticket_channel
    FROM labeled s
    JOIN tickets t ON t.agent_id = s.id AND t.requester_id > 7
)
SELECT *
FROM (
    SELECT tier, ticket_channel, hired_on_quarter
    FROM with_children
) src
PIVOT (
    SUM(hired_on_quarter) AS amount, COUNT(hired_on_quarter) AS n
    FOR ticket_channel IN ('web', 'phone', 'email', 'chat')
)
ORDER BY tier
