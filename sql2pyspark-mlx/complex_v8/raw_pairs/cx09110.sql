WITH base AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE csat >= 1
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.channel,
        s.csat,
        s.tags,
        su.agent_name,
        su.team AS agent_team,
        su.tier AS agent_tier
    FROM base s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        agent_id,
        csat,
        agent_team,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY MONTH(opened_at)), 2) AS requester_id_vs_opened_at_month_avg,
        COUNT(*) OVER w_agent_tier AS rows_in_agent_tier,
        DENSE_RANK() OVER w_agent_tier_csat_by_csat_ties AS csat_dense_rank,
        LAG(opened_at, 3) OVER w_all_by_csat AS prev3_opened_at,
        ROUND(AVG(requester_id) OVER (ORDER BY requester_id, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS requester_id_avg_3,
        SIZE(COLLECT_SET(channel) OVER w_agent_tier) AS distinct_channel_in_agent_tier,
        MIN(csat) OVER (ORDER BY csat DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_csat_so_far,
        RANK() OVER (PARTITION BY agent_tier, agent_team ORDER BY csat DESC, id) AS csat_rank,
        ROUND(csat - AVG(csat) OVER w_agent_tier, 2) AS csat_vs_agent_tier_avg,
        ROUND(AVG(requester_id) OVER (PARTITION BY agent_tier, agent_team ORDER BY csat DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS requester_id_avg_3_prior
    FROM combined_details
    WINDOW w_agent_tier AS (PARTITION BY agent_tier), w_agent_tier_csat_by_csat_ties AS (PARTITION BY agent_tier, csat ORDER BY csat DESC), w_all_by_csat AS (ORDER BY csat DESC NULLS FIRST, id)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(csat - AVG(csat) OVER w_agent_id_csat, 2) AS csat_vs_agent_id_csat_avg,
        ROW_NUMBER() OVER w_distinct_channel_in_agent_ti AS rn,
        ROUND(AVG(requester_id_avg_3) OVER (PARTITION BY DATE_TRUNC('MONTH', prev3_opened_at), distinct_channel_in_agent_tier ORDER BY requester_id_avg_3_prior ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS requester_id_avg_3_avg_2,
        MAX(min_csat_so_far) OVER w_agent_id_csat AS agent_id_csat_max_min_csat_so_far,
        ROUND(AVG(min_csat_so_far) OVER (PARTITION BY DATE_TRUNC('MONTH', prev3_opened_at), distinct_channel_in_agent_tier ORDER BY requester_id_avg_3_prior ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS min_csat_so_far_avg_1_prior,
        COUNT(*) OVER (PARTITION BY agent_id, csat) AS rows_in_agent_id_csat,
        csat_rank - LAG(csat_rank) OVER (PARTITION BY distinct_channel_in_agent_tier ORDER BY requester_id_avg_3 DESC, id) AS csat_rank_change,
        FIRST_VALUE(rows_in_agent_tier) OVER (PARTITION BY distinct_channel_in_agent_tier ORDER BY requester_id_avg_3 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_rows_in_agent_tier
    FROM metrics
    WINDOW w_agent_id_csat AS (PARTITION BY agent_id, csat), w_distinct_channel_in_agent_ti AS (PARTITION BY distinct_channel_in_agent_tier ORDER BY requester_id_avg_3 DESC, id)
)
-- Final projection
SELECT
    id,
    agent_id,
    csat,
    agent_team,
    requester_id_vs_opened_at_month_avg,
    csat_dense_rank,
    requester_id_avg_3,
    distinct_channel_in_agent_tier,
    csat_rank,
    csat_vs_agent_tier_avg,
    csat_vs_agent_id_csat_avg,
    rn,
    requester_id_avg_3_avg_2,
    agent_id_csat_max_min_csat_so_far,
    min_csat_so_far_avg_1_prior,
    csat_rank_change,
    first_rows_in_agent_tier,
    CASE WHEN agent_team IN ('Tech', 'Onboarding') THEN 'group_a' WHEN agent_team = 'VIP' THEN 'group_b' ELSE 'other' END AS agent_team_group
FROM with_trends
WHERE first_rows_in_agent_tier >= 4
ORDER BY agent_team DESC NULLS FIRST, distinct_channel_in_agent_tier, id
LIMIT 10
