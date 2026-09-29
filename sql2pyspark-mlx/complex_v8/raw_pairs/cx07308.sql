WITH stacked AS (
    -- Stack the branches
    SELECT tier AS label, team AS category
    FROM support_agents
    UNION ALL
    SELECT actor, event_type
    FROM ticket_events
)
SELECT label, COUNT(*) AS n_rows
FROM stacked
GROUP BY label
ORDER BY label
