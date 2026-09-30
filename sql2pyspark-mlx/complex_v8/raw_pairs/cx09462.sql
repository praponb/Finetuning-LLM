WITH prepared_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        channel,
        tags,
        DATEDIFF(DATE '2025-12-31', closed_at) AS days_since_closed_at,
        CONCAT(priority, '#', CAST(id AS STRING)) AS priority_ref
    FROM tickets
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        SUBSTRING(TRIM(channel), 1, 4) AS channel_prefix,
        CONCAT_WS('|', priority_ref, channel) AS priority_ref_channel_label,
        COALESCE(days_since_closed_at, 0) AS days_since_closed_at_filled
    FROM prepared_tickets
    WHERE channel = 'web'
)
-- Subtotals with CUBE
SELECT
    agent_id,
    COALESCE(channel, 'ALL') AS channel,
    ROUND(AVG(days_since_closed_at), 2) AS avg_days_since_closed_at
FROM tagged
GROUP BY CUBE(agent_id, channel)
ORDER BY agent_id NULLS LAST, channel NULLS LAST
