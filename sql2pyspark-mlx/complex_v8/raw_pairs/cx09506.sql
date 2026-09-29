WITH base_agents AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        team,
        tier,
        hired_on
    FROM support_agents
    WHERE team IN ('Onboarding', 'Billing', 'VIP')
)
SELECT
    tier,
    hired_on AS event_date,
    team AS label,
    'agent' AS record_type
FROM base_agents
WHERE team IN ('Billing', 'VIP', 'Tech')
  AND hired_on < DATE '2025-02-26'
UNION
SELECT
    team AS tier,
    hired_on AS event_date,
    agent_name AS label,
    'agent_2' AS record_type
FROM support_agents
WHERE hired_on BETWEEN DATE '2024-05-02' AND DATE '2025-03-30'
ORDER BY event_date, label
