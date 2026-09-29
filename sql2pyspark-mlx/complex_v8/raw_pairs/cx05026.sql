WITH filtered_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE closed_at >= TIMESTAMP '2025-01-15 08:00:00'
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.tags,
        su.agent_name,
        su.tier AS agent_tier
    FROM filtered_tickets s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
rollup_base AS (
    -- Summarise per group
    SELECT
        csat,
        priority,
        channel,
        COUNT(*) AS row_count,
        ROUND(100 * COUNT(CASE WHEN agent_tier = 'T3' THEN 1 END) / COUNT(*), 1) AS pct_agent_tier_t3,
        MIN(csat) AS min_csat
    FROM with_parent
    GROUP BY csat, priority, channel
    HAVING COUNT(*) >= 2
)
-- Final projection
SELECT
    csat,
    priority,
    channel,
    row_count,
    pct_agent_tier_t3,
    min_csat
FROM rollup_base
WHERE pct_agent_tier_t3 < 60.0
