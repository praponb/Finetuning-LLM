WITH scoped AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        tags,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 60, 1) AS minutes_opened_at_to_closed_at
    FROM tickets
    WHERE requester_id > 3
      AND closed_at >= TIMESTAMP '2025-01-25 11:00:00'
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        s.tags,
        su.agent_name
    FROM scoped s
    JOIN support_agents su ON su.id = s.agent_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY priority) AS rows_in_priority,
        ROUND(PERCENT_RANK() OVER (ORDER BY closed_at ASC, id), 3) AS closed_at_percent_rank
    FROM enriched
),
with_details_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.channel,
        s.agent_name,
        s.closed_at_percent_rank,
        su.team AS agent_team,
        su.tier AS agent_tier,
        su.agent_name AS agent_agent_name
    FROM metrics s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
agent_tier_stats AS (
    -- Median closed_at_percent_rank per agent_tier
    SELECT agent_tier, PERCENTILE_APPROX(closed_at_percent_rank, 0.5) AS median_closed_at_percent_rank
    FROM with_details_agent
    GROUP BY agent_tier
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_closed_at_percent_rank,
        ROUND(s.closed_at_percent_rank - g.median_closed_at_percent_rank, 2) AS closed_at_percent_rank_minus_median
    FROM with_details_agent s
    JOIN agent_tier_stats g ON g.agent_tier = s.agent_tier
    WHERE s.closed_at_percent_rank >= g.median_closed_at_percent_rank
),
grouped AS (
    -- Group-level totals
    SELECT
        channel,
        YEAR(closed_at) AS closed_at_year,
        agent_team,
        COUNT(*) AS cnt,
        MIN(closed_at_percent_rank_minus_median) AS min_closed_at_percent_rank_minus_median
    FROM outliers
    GROUP BY 1, 2, 3
)
-- Final projection
SELECT
    channel,
    agent_team,
    cnt,
    min_closed_at_percent_rank_minus_median,
    closed_at_year + cnt AS closed_at_year_plus_cnt
FROM grouped
ORDER BY closed_at_year_plus_cnt
