WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        LOWER(TRIM(team)) AS team_clean
    FROM support_agents
    WHERE UPPER(TRIM(agent_name)) LIKE 'J%'
      AND tier <> 'T1'
)
SELECT id AS key_id, hired_on AS event_date
FROM scoped
WHERE tier = 'T3'
UNION ALL
SELECT id, hired_on
FROM support_agents
WHERE agent_name IS NOT NULL
  AND hired_on < DATE '2025-03-30'
