WITH cleaned_agents AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        DATEDIFF(DATE '2025-06-30', hired_on) AS days_since_hired_on
    FROM support_agents
)
SELECT id
FROM cleaned_agents
WHERE tier = 'T2'
  AND team = 'Onboarding'
MINUS
SELECT id
FROM support_agents
WHERE hired_on < DATE '2024-12-31'
ORDER BY id
