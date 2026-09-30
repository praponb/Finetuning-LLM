WITH recent_agents AS (
    -- Filter support_agents
    SELECT
        id,
        agent_name,
        team,
        tier,
        LENGTH(TRIM(agent_name)) AS agent_name_len
    FROM support_agents
    WHERE UPPER(TRIM(agent_name)) LIKE 'G%'
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT MAX(t.requester_id) FROM tickets t WHERE t.agent_id = s.id) AS max_ticket_requester_id,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.id) AS ticket_count,
        (SELECT ROUND(AVG(t.csat), 2) FROM tickets t WHERE t.agent_id = s.id) AS avg_ticket_csat
    FROM recent_agents s
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT tier, team, agent_name_len
    FROM profiled
) src
PIVOT (
    MIN(agent_name_len) AS min_v, MAX(agent_name_len) AS max_v
    FOR team IN ('VIP', 'Onboarding', 'Tech', 'Billing')
)
ORDER BY tier
