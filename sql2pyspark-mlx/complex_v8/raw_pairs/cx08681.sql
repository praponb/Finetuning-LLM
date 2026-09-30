WITH filtered AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        tags,
        CASE WHEN csat >= 4 THEN 'A' WHEN csat >= 2 THEN 'B' ELSE 'C' END AS csat_tier
    FROM tickets
    WHERE requester_id <= 21
),
enriched_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.channel,
        s.tags,
        s.csat_tier,
        su.tier AS agent_tier
    FROM filtered s
    JOIN support_agents su ON su.id = s.agent_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(opened_at, 2) OVER (PARTITION BY agent_id ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_at,
        ROUND(PERCENT_RANK() OVER w_agent_id_by_opened_at, 3) AS opened_at_percent_rank,
        LEAD(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at DESC, id) AS next_opened_at,
        ROUND(CUME_DIST() OVER (PARTITION BY priority ORDER BY opened_at DESC), 3) AS opened_at_cume_dist,
        LAG(opened_at) OVER (PARTITION BY channel, agent_tier ORDER BY opened_at NULLS LAST, id) AS prev_opened_at,
        NTH_VALUE(opened_at, 2) OVER (PARTITION BY channel, agent_tier ORDER BY opened_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_at_2,
        LAST_VALUE(opened_at) IGNORE NULLS OVER (PARTITION BY priority ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_opened_at,
        ROUND(CUME_DIST() OVER (PARTITION BY channel, agent_tier ORDER BY opened_at NULLS LAST, id), 3) AS opened_at_cume_dist_2
    FROM enriched_agent
    WINDOW w_agent_id_by_opened_at AS (PARTITION BY agent_id ORDER BY opened_at DESC, id)
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(opened_at_percent_rank, 0.0) AS opened_at_percent_rank_filled,
        opened_at_percent_rank + opened_at_cume_dist_2 AS opened_at_percent_rank_plus_opened_at_cume_dist_,
        CASE WHEN opened_at_cume_dist >= 0.6 THEN 'HIGH' WHEN opened_at_cume_dist >= 0.5 THEN 'MEDIUM' ELSE 'LOW' END AS opened_at_cume_dist_tier
    FROM with_trends
    WHERE opened_at >= TIMESTAMP '2025-01-13 07:00:00'
),
with_trends_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        opened_at,
        priority,
        tags,
        csat_tier,
        agent_tier,
        second_opened_at,
        opened_at_cume_dist,
        prev_opened_at,
        opened_at_cume_dist_2,
        opened_at_percent_rank_filled,
        opened_at_cume_dist_tier,
        LAG(opened_at_percent_rank, 2, 0) OVER (ORDER BY opened_at_percent_rank_plus_opened_at_cume_dist_, id) AS prev2_opened_at_percent_rank,
        DENSE_RANK() OVER (ORDER BY opened_at_percent_rank_plus_opened_at_cume_dist_) AS opened_at_percent_rank_plus_opened_at_cume_dist__2,
        LEAD(opened_at) OVER (PARTITION BY priority, channel ORDER BY next_opened_at NULLS LAST, id) AS next_opened_at_2,
        SUM(opened_at_cume_dist_2) OVER w_csat_tier AS csat_tier_sum_opened_at_cume_dist_2,
        LAST_VALUE(opened_at_cume_dist_2) IGNORE NULLS OVER (PARTITION BY priority, channel ORDER BY next_opened_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_opened_at_cume_dist_2,
        ROUND(opened_at_cume_dist / NULLIF(SUM(opened_at_cume_dist) OVER (PARTITION BY csat_tier), 0), 4) AS opened_at_cume_dist_share_of_csat_tier,
        DENSE_RANK() OVER w_priority_channel_by_next_ope_3 AS next_opened_at_dense_rank,
        MAX(opened_at_percent_rank) OVER (PARTITION BY priority, channel ORDER BY next_opened_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_opened_at_percent_rank_so_far
    FROM annotated
    WINDOW w_csat_tier AS (PARTITION BY csat_tier), w_priority_channel_by_next_ope_3 AS (PARTITION BY priority, channel ORDER BY next_opened_at NULLS LAST)
)
-- Final projection
SELECT
    id,
    opened_at,
    priority,
    tags,
    csat_tier,
    agent_tier,
    opened_at_cume_dist,
    opened_at_cume_dist_2,
    opened_at_cume_dist_tier,
    prev2_opened_at_percent_rank,
    opened_at_percent_rank_plus_opened_at_cume_dist__2,
    next_opened_at_2,
    csat_tier_sum_opened_at_cume_dist_2,
    last_known_opened_at_cume_dist_2,
    opened_at_cume_dist_share_of_csat_tier,
    max_opened_at_percent_rank_so_far,
    TO_DATE(opened_at) AS opened_at_date
FROM with_trends_2
ORDER BY last_known_opened_at_cume_dist_2 DESC
