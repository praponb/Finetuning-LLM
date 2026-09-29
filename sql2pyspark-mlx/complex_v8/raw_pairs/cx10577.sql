WITH valid_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        requester_id + csat AS requester_id_plus_csat
    FROM tickets
    WHERE agent_id <= 7
),
combined_details_agent AS (
    -- Enrich with dimension data
    SELECT s.*, su.hired_on AS agent_hired_on, su.tier AS agent_tier
    FROM valid_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY agent_tier), 0), 4) AS csat_share_of_agent_tier,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY agent_tier)) AS distinct_agent_id_in_agent_tier,
        ROW_NUMBER() OVER w_closed_at_month_csat_by_agen AS seq_no,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY priority)) AS distinct_agent_id_in_priority,
        SIZE(COLLECT_SET(agent_tier) OVER w_priority) AS distinct_agent_tier_in_priority,
        ROUND(csat - AVG(csat) OVER (PARTITION BY agent_tier), 2) AS csat_vs_agent_tier_avg,
        SIZE(COLLECT_SET(channel) OVER (PARTITION BY priority)) AS distinct_channel_in_priority,
        MAX(requester_id_plus_csat) OVER (ORDER BY agent_hired_on, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS requester_id_plus_csat_max_6_prior,
        NTH_VALUE(agent_hired_on, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at), csat ORDER BY agent_hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_agent_hired_on,
        DATEDIFF(agent_hired_on, LAG(agent_hired_on) OVER w_all_by_agent_hired_on) AS days_since_prev_agent_hired_on,
        LAST_VALUE(closed_at) IGNORE NULLS OVER (PARTITION BY agent_tier ORDER BY requester_id_plus_csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_closed_at,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at), csat ORDER BY agent_hired_on, id), 3) AS agent_hired_on_cume_dist,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY channel, csat ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at
    FROM combined_details_agent
    WINDOW w_closed_at_month_csat_by_agen AS (PARTITION BY DATE_TRUNC('MONTH', closed_at), csat ORDER BY agent_hired_on, id), w_priority AS (PARTITION BY priority), w_all_by_agent_hired_on AS (ORDER BY agent_hired_on, id)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(requester_id_plus_csat) OVER w_agent_tier_distinct_agent_ti AS agent_tier_distinct_agent_tier_in_priority_max_r,
        NTILE(10) OVER (PARTITION BY distinct_agent_id_in_agent_tier ORDER BY requester_id_plus_csat_max_6_prior, id) AS requester_id_plus_csat_max_6_prior_decile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY distinct_agent_id_in_agent_tier ORDER BY requester_id_plus_csat_max_6_prior), 3) AS requester_id_plus_csat_max_6_prior_percent_rank,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY agent_tier, distinct_agent_tier_in_priority)) AS distinct_agent_id_in_agent_tier_distinct_agent_t,
        DENSE_RANK() OVER (ORDER BY days_since_prev_agent_hired_on DESC, id) AS days_since_prev_agent_hired_on_dense_rank
    FROM ranked
    WINDOW w_agent_tier_distinct_agent_ti AS (PARTITION BY agent_tier, distinct_agent_tier_in_priority)
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        csat,
        distinct_agent_id_in_agent_tier_distinct_agent_t,
        SUM(CASE WHEN channel = 'web' THEN 1 ELSE 0 END) AS n_channel_web,
        SUM(CASE WHEN agent_tier <> 'T3' THEN 1 ELSE 0 END) AS n_agent_tier_t3,
        ARRAY_SORT(COLLECT_SET(channel)) AS channel_list,
        COUNT(*) AS cnt,
        SUM(distinct_agent_id_in_agent_tier) FILTER (WHERE priority IS NOT NULL) AS distinct_agent_id_in_agent_tier_priority_is_not_,
        ROUND(SUM(csat) / NULLIF(SUM(agent_tier_distinct_agent_tier_in_priority_max_r), 0), 3) AS csat_to_agent_tier_distinct_agent_tier_in_priori
    FROM windowed
    WHERE distinct_agent_id_in_priority < 9
    GROUP BY csat, distinct_agent_id_in_agent_tier_distinct_agent_t
    HAVING COUNT(*) >= 2
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        csat,
        distinct_agent_id_in_agent_tier_distinct_agent_t,
        cnt,
        distinct_agent_id_in_agent_tier_priority_is_not_,
        FIRST_VALUE(csat) OVER (PARTITION BY n_channel_web ORDER BY cnt, csat, distinct_agent_id_in_agent_tier_distinct_agent_t ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_csat,
        LAG(cnt, 1, 0) OVER (PARTITION BY n_channel_web ORDER BY cnt, csat, distinct_agent_id_in_agent_tier_distinct_agent_t) AS prev_cnt,
        NTILE(3) OVER (PARTITION BY n_channel_web ORDER BY cnt, csat, distinct_agent_id_in_agent_tier_distinct_agent_t) AS cnt_tercile,
        SUM(csat) OVER (PARTITION BY n_channel_web ORDER BY cnt, csat, distinct_agent_id_in_agent_tier_distinct_agent_t ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS csat_sum_2_prior
    FROM totals
)
-- Final projection
SELECT
    csat,
    distinct_agent_id_in_agent_tier_distinct_agent_t,
    cnt,
    distinct_agent_id_in_agent_tier_priority_is_not_,
    csat_sum_2_prior,
    COALESCE(first_csat, 0) AS first_csat_filled,
    cnt + csat AS cnt_plus_csat
FROM with_trends
ORDER BY csat DESC NULLS FIRST, csat_sum_2_prior DESC, distinct_agent_id_in_agent_tier_distinct_agent_t
LIMIT 20
