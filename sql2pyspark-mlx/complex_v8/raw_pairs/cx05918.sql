WITH filtered_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        channel,
        tags,
        CONCAT(channel, '#', CAST(csat AS STRING)) AS channel_ref,
        DAYOFWEEK(opened_at) AS opened_at_dayofweek,
        SUBSTRING(TRIM(priority), 1, 3) AS priority_prefix
    FROM tickets
    WHERE agent_id IS NOT NULL
      AND SIZE(tags) >= 2
),
combined_details_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.channel,
        s.priority_prefix,
        su.agent_name,
        su.tier AS agent_tier
    FROM filtered_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(closed_at) OVER (PARTITION BY channel ORDER BY closed_at ASC NULLS LAST, id) AS prev_closed_at,
        ROUND(CUME_DIST() OVER (PARTITION BY channel, agent_id ORDER BY closed_at), 3) AS closed_at_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY channel ORDER BY closed_at ASC NULLS LAST), 3) AS closed_at_percent_rank
    FROM combined_details_agent
)
-- Final projection
SELECT
    id,
    closed_at,
    channel,
    priority_prefix,
    agent_name,
    agent_tier,
    closed_at_percent_rank,
    LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug,
    GREATEST(closed_at_percent_rank, closed_at_cume_dist) AS greatest_closed_at_percent_rank_closed_at_cume_d
FROM metrics
ORDER BY agent_name, agent_tier
