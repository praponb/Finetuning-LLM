WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        tags,
        COALESCE(channel, 'UNKNOWN') AS channel_or_unknown,
        DATE_TRUNC('MONTH', opened_at) AS opened_at_month
    FROM tickets
    WHERE agent_id >= 6
      AND TO_DATE(closed_at) < DATE '2025-02-24'
),
combined_details AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.tags,
        s.opened_at_month,
        su.hired_on AS agent_hired_on
    FROM prepared s
    JOIN support_agents su ON su.id = s.agent_id
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    tags,
    opened_at_month,
    agent_hired_on,
    ADD_MONTHS(agent_hired_on, 12) AS agent_hired_on_plus_12m,
    ABS(requester_id - 8) AS requester_id_distance
FROM combined_details
ORDER BY agent_hired_on_plus_12m DESC
