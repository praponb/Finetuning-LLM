WITH recent AS (
    -- Filter support_agents
    SELECT
        id,
        agent_name,
        team,
        hired_on,
        LOWER(REGEXP_REPLACE(TRIM(team), ' ', '_')) AS team_slug
    FROM support_agents
    WHERE LOWER(agent_name) LIKE '%wa%'
)
SELECT id AS key_id, team_slug AS label, 'agent' AS origin
FROM recent
UNION ALL
SELECT id, channel, 'ticket' AS origin
FROM tickets
