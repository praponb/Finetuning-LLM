WITH all_events AS (
    -- Stack the branches
    SELECT agent_name, hired_on AS day, 'agent' AS record_type
    FROM support_agents
    WHERE team IN ('Tech', 'Onboarding', 'Billing')
    UNION ALL
    SELECT team AS agent_name, hired_on AS day, 'agent_2' AS record_type
    FROM support_agents
    UNION ALL
    SELECT tier AS agent_name, hired_on AS day, 'agent_3' AS record_type
    FROM support_agents
)
SELECT record_type, COUNT(*) AS n_rows, MAX(day) AS latest_day
FROM all_events
GROUP BY record_type
ORDER BY record_type
