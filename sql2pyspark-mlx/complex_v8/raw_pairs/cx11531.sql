WITH base_agents AS (
    -- Filter support_agents
    SELECT
        id,
        agent_name,
        team,
        tier
    FROM support_agents
    WHERE UPPER(TRIM(agent_name)) LIKE 'F%'
      AND hired_on >= DATE '2024-07-09'
),
with_activity AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM base_agents s
    WHERE s.id IN (
            SELECT t.agent_id
            FROM tickets t
            WHERE t.requester_id BETWEEN 2 AND 25
              AND t.opened_at < TIMESTAMP '2025-02-24 08:00:00'
        )
),
with_activity_2 AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM with_activity s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.id
              AND t.csat > 3
              AND t.channel = 'phone'
        )
)
SELECT id, agent_name, tier
FROM with_activity_2
ORDER BY agent_name NULLS LAST
