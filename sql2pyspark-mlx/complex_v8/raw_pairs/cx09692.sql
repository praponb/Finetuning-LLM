WITH cleaned_tickets AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        csat,
        tags
    FROM tickets
    WHERE (agent_id >= 2 OR requester_id < 23)
)
SELECT channel AS name
FROM tickets
WHERE csat < 4
  AND requester_id > 11
INTERSECT
SELECT team
FROM support_agents
WHERE LOWER(agent_name) LIKE '%ha%'
  AND team IN ('VIP', 'Billing', 'Onboarding')
ORDER BY name
