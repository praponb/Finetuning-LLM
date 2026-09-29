WITH scoped AS (
    -- Scope the support_agents rows
    SELECT
        id,
        agent_name,
        team,
        hired_on,
        CONCAT(agent_name, '#', CAST(id AS STRING)) AS agent_name_ref,
        ADD_MONTHS(hired_on, 3) AS hired_on_plus_3m
    FROM support_agents
    WHERE hired_on >= DATE '2024-07-28'
      AND UPPER(TRIM(agent_name)) LIKE 'K%'
),
with_counts AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT MAX(t.closed_at) FROM tickets t WHERE t.agent_id = s.id) AS last_ticket_closed_at
    FROM scoped s
)
SELECT
    id,
    agent_name,
    hired_on,
    agent_name_ref,
    last_ticket_closed_at,
    DATEDIFF(hired_on, hired_on_plus_3m) AS days_hired_on_plus_3m_to_hired_on,
    DATEDIFF(DATE '2025-12-31', last_ticket_closed_at) AS days_since_last_ticket_closed_at
FROM with_counts
ORDER BY hired_on DESC
