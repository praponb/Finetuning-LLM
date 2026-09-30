WITH prepared_agents AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        LENGTH(TRIM(agent_name)) AS agent_name_len
    FROM support_agents
    WHERE tier IN ('T2', 'T1')
      AND LENGTH(TRIM(agent_name)) >= 5
)
SELECT
    id AS key_id,
    tier,
    hired_on AS event_date,
    'agent' AS record_type
FROM prepared_agents
WHERE team IN ('Billing', 'VIP', 'Onboarding')
UNION ALL
SELECT
    id AS key_id,
    tier,
    hired_on AS event_date,
    'agent_2' AS record_type
FROM support_agents
UNION ALL
SELECT
    id AS key_id,
    agent_name AS tier,
    hired_on AS event_date,
    'agent_3' AS record_type
FROM support_agents
