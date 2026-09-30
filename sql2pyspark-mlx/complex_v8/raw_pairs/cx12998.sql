WITH valid_tickets AS (
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
        COALESCE(requester_id, 0) AS requester_id_filled,
        COALESCE(channel, 'UNKNOWN') AS channel_or_unknown
    FROM tickets
    WHERE priority IS NOT NULL
),
with_details AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.channel,
        s.csat,
        s.tags,
        s.channel_or_unknown,
        su.tier AS agent_tier
    FROM valid_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        requester_id,
        agent_id,
        channel,
        csat,
        agent_tier,
        ROW_NUMBER() OVER (PARTITION BY agent_tier ORDER BY opened_at DESC NULLS FIRST, id) AS opened_at_seq,
        MIN(requester_id) OVER (PARTITION BY agent_id, channel ORDER BY opened_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_requester_id_so_far,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at), csat), 0), 4) AS requester_id_share_of_opened_at_month_csat,
        ROUND(csat - AVG(csat) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at), csat), 2) AS csat_vs_opened_at_month_csat_avg,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at), csat) AS rows_in_opened_at_month_csat,
        MAX(requester_id) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at), csat) AS opened_at_month_csat_max_requester_id,
        DENSE_RANK() OVER (PARTITION BY agent_id, channel ORDER BY opened_at ASC) AS opened_at_dense_rank,
        LAST_VALUE(closed_at) IGNORE NULLS OVER (PARTITION BY agent_tier ORDER BY opened_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_closed_at,
        SUM(requester_id) OVER (PARTITION BY agent_tier ORDER BY opened_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_requester_id,
        ROUND(AVG(csat) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at), csat), 2) AS opened_at_month_csat_avg_csat,
        SUM(requester_id) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at), csat) AS opened_at_month_csat_sum_requester_id,
        ROUND(AVG(csat) OVER (PARTITION BY channel_or_unknown ORDER BY opened_at NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS csat_avg_3_prior
    FROM with_details
),
per_group AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', last_known_closed_at) AS last_known_closed_at_month,
        agent_tier,
        PERCENTILE_APPROX(csat, 0.5) AS median_csat,
        ARRAY_SORT(COLLECT_SET(agent_tier)) AS agent_tier_list,
        COUNT(*) AS cnt
    FROM with_trends
    GROUP BY 1, 2
    HAVING MAX(opened_at_month_csat_max_requester_id) > 26
)
-- Final projection
SELECT
    last_known_closed_at_month,
    agent_tier,
    median_csat,
    agent_tier_list,
    cnt,
    COALESCE(agent_tier, 'UNKNOWN') AS agent_tier_or_unknown,
    ABS(cnt - 5) AS cnt_distance
FROM per_group
ORDER BY cnt_distance DESC
