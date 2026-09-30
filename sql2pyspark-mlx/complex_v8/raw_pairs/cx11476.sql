WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        csat,
        tags
    FROM tickets
    WHERE (channel = 'web' OR csat > 1)
),
with_details AS (
    -- Join support_agents
    SELECT
        s.*,
        su.hired_on AS agent_hired_on,
        su.agent_name,
        su.team AS agent_team
    FROM cleaned s
    JOIN support_agents su ON su.id = s.agent_id
),
latest_per_key AS (
    -- Keep the latest row per agent_team, channel
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        csat,
        tags,
        agent_hired_on,
        agent_name,
        agent_team
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_team, channel ORDER BY closed_at DESC, id DESC) AS row_num
        FROM with_details
    ) ranked
    WHERE row_num = 1
),
filtered AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM latest_per_key s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.opened_at >= TIMESTAMP '2025-01-13 11:00:00'
        )
)
SELECT
    id,
    agent_id,
    channel,
    csat,
    agent_hired_on,
    agent_team
FROM filtered
WHERE agent_hired_on IS NOT NULL
ORDER BY agent_team DESC, channel DESC
