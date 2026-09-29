WITH prepared_agents AS (
    -- Filter support_agents
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        INITCAP(TRIM(agent_name)) AS agent_name_clean
    FROM support_agents
    WHERE tier IS NOT NULL
      AND hired_on >= DATE '2024-04-14'
)
SELECT
    agent_name AS label,
    id AS key_id,
    hired_on AS day,
    'agent' AS record_type
FROM prepared_agents
UNION ALL
SELECT
    tier AS label,
    id AS key_id,
    hired_on AS day,
    'agent_2' AS record_type
FROM support_agents
WHERE LENGTH(TRIM(agent_name)) >= 4
