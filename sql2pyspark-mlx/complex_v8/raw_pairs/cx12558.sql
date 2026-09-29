WITH base_tickets AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        priority,
        channel,
        csat
    FROM tickets
    WHERE priority = 'P2'
),
joined AS (
    -- Enrich with dimension data
    SELECT s.*, su.tier AS agent_tier
    FROM base_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY csat DESC), 3) AS csat_percent_rank,
        ROUND(csat - AVG(csat) OVER w_agent_tier, 2) AS csat_vs_agent_tier_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY agent_id, csat ORDER BY csat DESC NULLS FIRST), 3) AS csat_percent_rank_2,
        NTH_VALUE(csat, 2) OVER (PARTITION BY agent_id, csat ORDER BY csat DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY priority), 0), 4) AS csat_share_of_priority,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY agent_tier), 0), 4) AS csat_share_of_agent_tier,
        RANK() OVER (ORDER BY csat DESC) AS csat_rank,
        DENSE_RANK() OVER w_csat_channel_by_csat_ties AS csat_dense_rank,
        csat - LAG(csat) OVER (PARTITION BY csat, channel ORDER BY csat DESC, id) AS csat_change,
        ROUND(CUME_DIST() OVER w_agent_id_csat_by_csat_ties, 3) AS csat_cume_dist,
        LEAD(csat, 2) OVER (ORDER BY csat DESC, id) AS next2_csat,
        COUNT(*) OVER (PARTITION BY agent_tier) AS rows_in_agent_tier,
        ROUND(CUME_DIST() OVER (PARTITION BY agent_id, csat ORDER BY csat DESC NULLS FIRST, id), 3) AS csat_cume_dist_2,
        NTH_VALUE(csat, 2) OVER (ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat_2
    FROM joined
    WINDOW w_agent_tier AS (PARTITION BY agent_tier), w_csat_channel_by_csat_ties AS (PARTITION BY csat, channel ORDER BY csat DESC), w_agent_id_csat_by_csat_ties AS (PARTITION BY agent_id, csat ORDER BY csat DESC NULLS FIRST)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(agent_tier) OVER (PARTITION BY agent_id, csat_dense_rank)) AS distinct_agent_tier_in_agent_id_csat_dense_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY csat_dense_rank ORDER BY csat_cume_dist_2 ASC), 3) AS csat_cume_dist_2_percent_rank,
        MIN(csat_cume_dist_2) OVER (PARTITION BY csat_rank ORDER BY second_csat_2 NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_csat_cume_dist_2_so_far,
        MIN(second_csat_2) OVER (PARTITION BY csat_dense_rank ORDER BY csat_cume_dist_2 ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_second_csat_2_so_far,
        ROUND(csat_share_of_agent_tier / NULLIF(SUM(csat_share_of_agent_tier) OVER w_agent_id_csat_dense_rank, 0), 4) AS csat_share_of_agent_tier_share_of_agent_id_csat_
    FROM metrics
    WINDOW w_agent_id_csat_dense_rank AS (PARTITION BY agent_id, csat_dense_rank)
)
SELECT
    id,
    agent_id,
    priority,
    channel,
    csat,
    agent_tier,
    csat_percent_rank,
    csat_vs_agent_tier_avg,
    csat_percent_rank_2,
    second_csat,
    csat_share_of_priority,
    csat_rank,
    csat_dense_rank,
    csat_change,
    csat_cume_dist,
    next2_csat,
    rows_in_agent_tier,
    csat_cume_dist_2,
    second_csat_2,
    csat_cume_dist_2_percent_rank,
    min_csat_cume_dist_2_so_far,
    min_second_csat_2_so_far,
    csat_share_of_agent_tier_share_of_agent_id_csat_
FROM windowed
ORDER BY priority
