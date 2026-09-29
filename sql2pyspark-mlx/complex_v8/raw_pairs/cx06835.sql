WITH prepared AS (
    -- Keep valid rows and derive helper columns
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
        SUBSTRING(TRIM(channel), 1, 4) AS channel_prefix
    FROM tickets
    WHERE priority NOT IN ('P4', 'P2')
      AND agent_id IS NOT NULL
),
joined_agent AS (
    -- Join support_agents
    SELECT s.*, su.tier AS agent_tier
    FROM prepared s
    JOIN support_agents su ON su.id = s.agent_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY channel)) AS distinct_priority_in_channel,
        SUM(csat) OVER (PARTITION BY agent_tier) AS agent_tier_sum_csat,
        DENSE_RANK() OVER w_all_by_opened_at_ties AS opened_at_dense_rank,
        MAX(requester_id) OVER (PARTITION BY agent_tier) AS agent_tier_max_requester_id,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY agent_tier), 0), 4) AS requester_id_share_of_agent_tier,
        ROUND(csat - AVG(csat) OVER (PARTITION BY channel), 2) AS csat_vs_channel_avg,
        RANK() OVER (ORDER BY opened_at DESC, id) AS opened_at_rank,
        COUNT(*) OVER w_channel AS rows_in_channel,
        MAX(requester_id) OVER (PARTITION BY agent_id, TO_DATE(opened_at) ORDER BY csat NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS requester_id_max_2,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY channel), 0), 4) AS requester_id_share_of_channel
    FROM joined_agent
    WINDOW w_all_by_opened_at_ties AS (ORDER BY opened_at DESC), w_channel AS (PARTITION BY channel)
),
first_rows AS (
    -- Top 5 per group
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
        channel_prefix,
        agent_tier,
        distinct_priority_in_channel,
        agent_tier_sum_csat,
        opened_at_dense_rank,
        agent_tier_max_requester_id,
        requester_id_share_of_agent_tier,
        csat_vs_channel_avg,
        rows_in_channel,
        requester_id_max_2,
        requester_id_share_of_channel
    FROM windowed
    WHERE opened_at_rank <= 5
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        opened_at_dense_rank,
        ARRAY_SORT(COLLECT_SET(agent_tier)) AS agent_tier_list,
        PERCENTILE_APPROX(agent_tier_sum_csat, 0.5) AS median_agent_tier_sum_csat
    FROM first_rows
    GROUP BY opened_at_dense_rank
),
windowed_2 AS (
    -- Running and moving aggregates
    SELECT
        opened_at_dense_rank,
        agent_tier_list,
        ROUND(PERCENT_RANK() OVER (ORDER BY median_agent_tier_sum_csat), 3) AS median_agent_tier_sum_csat_percent_rank,
        median_agent_tier_sum_csat - LAG(median_agent_tier_sum_csat) OVER (ORDER BY median_agent_tier_sum_csat, opened_at_dense_rank) AS median_agent_tier_sum_csat_change
    FROM per_group
)
-- Final projection
SELECT
    opened_at_dense_rank,
    ABS(opened_at_dense_rank - 6) AS opened_at_dense_rank_distance
FROM windowed_2
ORDER BY opened_at_dense_rank DESC, opened_at_dense_rank_distance
