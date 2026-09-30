WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        csat,
        SUBSTRING(TRIM(channel), 1, 3) AS channel_prefix,
        CONCAT(channel, '#', CAST(requester_id AS STRING)) AS channel_ref
    FROM tickets
    WHERE MONTH(opened_at) IN (1, 2)
      AND channel IN ('chat', 'email', 'phone')
),
latest AS (
    -- Keep the latest row per agent_id
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        csat,
        channel_prefix,
        channel_ref
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY csat DESC, id DESC) AS row_num
        FROM prepared
    ) ranked
    WHERE row_num = 1
)
-- Subtotals with CUBE
SELECT
    csat,
    agent_id,
    COUNT(*) AS cnt
FROM latest
GROUP BY CUBE(csat, agent_id)
ORDER BY csat NULLS LAST, agent_id NULLS LAST
