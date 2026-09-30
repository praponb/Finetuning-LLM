WITH valid_agents AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_name,
        team,
        tier
    FROM support_agents
    WHERE team IN ('Tech', 'Billing')
)
SELECT id
FROM valid_agents
WHERE agent_name IS NOT NULL
EXCEPT
SELECT id
FROM ticket_events
ORDER BY id DESC
