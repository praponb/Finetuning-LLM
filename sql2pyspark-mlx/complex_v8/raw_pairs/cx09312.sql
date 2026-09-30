WITH base_agents AS (
    -- Scope the support_agents rows
    SELECT *
    FROM support_agents
    WHERE team IN ('VIP', 'Onboarding', 'Billing')
),
combined AS (
    -- Stack the branches
    SELECT agent_name AS category, hired_on
    FROM base_agents
    UNION ALL
    SELECT agent_name AS category, hired_on
    FROM support_agents
)
SELECT category, COUNT(*) AS n_rows, MAX(hired_on) AS latest_hired_on
FROM combined
GROUP BY category
ORDER BY category
