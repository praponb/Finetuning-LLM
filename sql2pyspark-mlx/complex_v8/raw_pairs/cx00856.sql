WITH scoped AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        priority,
        channel,
        csat
    FROM tickets
    WHERE channel IN ('phone', 'web')
),
combined_details_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.priority,
        s.channel,
        s.csat,
        su.agent_name,
        su.tier AS agent_tier
    FROM scoped s
    JOIN support_agents su ON su.id = s.agent_id AND su.tier IN ('T3', 'T2')
    WHERE UPPER(TRIM(su.agent_name)) LIKE 'F%'
),
combined_details AS (
    -- Join support_agents
    SELECT s.*, su.tier AS agent_tier_2, su.agent_name AS agent_agent_name
    FROM combined_details_agent s
    JOIN support_agents su ON su.id = s.agent_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(csat) IGNORE NULLS OVER (PARTITION BY agent_tier_2 ORDER BY csat ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_csat,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY agent_tier, priority), 0), 4) AS csat_share_of_agent_tier_priority,
        ROUND(csat - AVG(csat) OVER (PARTITION BY agent_tier, priority), 2) AS csat_vs_agent_tier_priority_avg,
        LAG(csat) OVER (PARTITION BY agent_tier_2 ORDER BY csat ASC, id) AS prev_csat,
        SUM(csat) OVER (ORDER BY csat DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_csat
    FROM combined_details
    WINDOW w_agent_tier_priority AS (PARTITION BY agent_tier, priority)
)
-- Subtotals with ROLLUP
SELECT
    prev_csat,
    last_known_csat,
    ROUND(AVG(csat_share_of_agent_tier_priority), 2) AS avg_csat_share_of_agent_tier_priority
FROM ranked
GROUP BY ROLLUP(prev_csat, last_known_csat)
