WITH unioned AS (
    -- Stack the branches
    SELECT 'agent' AS record_type, id AS entity_id, hired_on
    FROM support_agents
    UNION ALL
    SELECT 'agent_2' AS record_type, id, hired_on
    FROM support_agents
    WHERE LENGTH(TRIM(agent_name)) >= 8
)
SELECT record_type, COUNT(*) AS n_rows, SUM(entity_id) AS total_entity_id, MAX(hired_on) AS latest_hired_on
FROM unioned
GROUP BY record_type
ORDER BY record_type
