WITH stacked AS (
    -- Stack the branches
    SELECT actor AS label, event_type, 'event' AS src
    FROM ticket_events
    UNION ALL
    SELECT team, agent_name, 'agent' AS src
    FROM support_agents
    UNION ALL
    SELECT agent_name, tier, 'agent' AS src
    FROM support_agents
    WHERE tier = 'T1'
      AND UPPER(TRIM(agent_name)) LIKE 'K%'
)
SELECT src, COUNT(*) AS n_rows
FROM stacked
GROUP BY src
ORDER BY src
