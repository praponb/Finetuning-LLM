WITH base AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 60, 1) AS minutes_opened_at_to_closed_at
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'outage')
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        LENGTH(TRIM(channel)) AS channel_len,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
    FROM base
    WHERE TO_DATE(closed_at) < DATE '2025-03-01'
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        COUNT(CASE WHEN channel IN ('phone', 'chat', 'web') THEN 1 END) AS ticket_channel_count
    FROM tickets
    GROUP BY agent_id
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_channel_count, 0) AS ticket_channel_count
    FROM enriched s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
)
-- Subtotals with CUBE
SELECT
    agent_id,
    csat,
    COALESCE(priority, 'ALL') AS priority,
    GROUPING(agent_id) AS is_total_agent_id,
    ROUND(SUM(ticket_channel_count) / NULLIF(SUM(csat), 0), 3) AS ticket_channel_count_to_csat_ratio
FROM with_stats
GROUP BY CUBE(agent_id, csat, priority)
ORDER BY agent_id NULLS LAST, csat NULLS LAST, priority NULLS LAST
