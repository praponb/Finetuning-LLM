WITH all_events AS (
    -- Stack the branches
    SELECT
        'agent' AS record_type,
        tier,
        hired_on,
        agent_name AS category
    FROM support_agents
    WHERE team = 'VIP'
      AND LOWER(agent_name) LIKE '%fa%'
    UNION ALL
    SELECT
        'agent_2' AS record_type,
        team AS tier,
        hired_on,
        agent_name AS category
    FROM support_agents
    WHERE tier = 'T1'
    UNION ALL
    SELECT
        'agent_3' AS record_type,
        team AS tier,
        hired_on,
        tier AS category
    FROM support_agents
)
SELECT record_type, COUNT(*) AS n_rows, MAX(hired_on) AS latest_hired_on
FROM all_events
GROUP BY record_type
ORDER BY record_type
