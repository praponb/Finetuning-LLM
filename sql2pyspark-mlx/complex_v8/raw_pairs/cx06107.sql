WITH base_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        tags,
        FLOOR(requester_id / 5) * 5 AS requester_id_bucket,
        CASE WHEN channel IN ('phone', 'web') THEN 'group_a' WHEN channel = 'email' THEN 'group_b' ELSE 'other' END AS channel_group
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'refund')
),
with_details_agent AS (
    -- Attach agent attributes
    SELECT
        s.*,
        su.team AS agent_team,
        su.agent_name,
        su.tier AS agent_tier
    FROM base_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY channel ORDER BY requester_id ASC NULLS LAST, id) AS requester_id_quintile,
        FIRST_VALUE(requester_id_bucket) OVER (PARTITION BY channel ORDER BY requester_id ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_requester_id_bucket,
        LEAD(requester_id) OVER (PARTITION BY channel_group ORDER BY closed_at, id) AS next_requester_id,
        ROUND(AVG(requester_id) OVER (PARTITION BY channel), 2) AS channel_avg_requester_id,
        SUM(requester_id) OVER (PARTITION BY channel_group ORDER BY closed_at, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS requester_id_sum_1_prior,
        MIN(requester_id) OVER (PARTITION BY MONTH(closed_at)) AS closed_at_month_min_requester_id,
        LEAD(requester_id_bucket, 3, 0) OVER (PARTITION BY channel ORDER BY requester_id, id) AS next3_requester_id_bucket,
        ROUND(requester_id_bucket - AVG(requester_id_bucket) OVER (PARTITION BY MONTH(closed_at)), 2) AS requester_id_bucket_vs_closed_at_month_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY channel ORDER BY requester_id ASC NULLS LAST), 3) AS requester_id_percent_rank,
        FIRST_VALUE(requester_id_bucket) IGNORE NULLS OVER (PARTITION BY agent_tier ORDER BY requester_id_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_requester_id_bucket,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER w_channel_group, 0), 4) AS requester_id_share_of_channel_group
    FROM with_details_agent
    WINDOW w_channel_group AS (PARTITION BY channel_group)
),
annotated AS (
    -- Add calculated columns
    SELECT *, COALESCE(requester_id_bucket, 0) AS requester_id_bucket_filled
    FROM windowed
    WHERE channel_group IN ('other', 'group_b')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(requester_id_sum_1_prior) OVER w_agent_team_requester_id_quin AS agent_team_requester_id_quintile_sum_requester_i,
        ROUND(requester_id_bucket_filled - AVG(requester_id_bucket_filled) OVER (PARTITION BY agent_team, requester_id_quintile), 2) AS requester_id_bucket_filled_vs_agent_team_request,
        SIZE(COLLECT_SET(priority) OVER w_agent_team_requester_id_quin) AS distinct_priority_in_agent_team_requester_id_qui,
        next3_requester_id_bucket - LAG(next3_requester_id_bucket) OVER (PARTITION BY channel_group, agent_id ORDER BY requester_id_sum_1_prior DESC NULLS FIRST, id) AS next3_requester_id_bucket_change,
        MIN(first_requester_id_bucket) OVER (PARTITION BY channel_group, agent_id ORDER BY requester_id_sum_1_prior DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_first_requester_id_bucket_so_far,
        NTH_VALUE(requester_id_quintile, 2) OVER (PARTITION BY channel_group, agent_id ORDER BY requester_id_sum_1_prior DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id_quintile,
        LAG(closed_at) OVER w_all_by_requester_id_bucket_v AS prev_closed_at
    FROM annotated
    WINDOW w_agent_team_requester_id_quin AS (PARTITION BY agent_team, requester_id_quintile), w_all_by_requester_id_bucket_v AS (ORDER BY requester_id_bucket_vs_closed_at_month_avg ASC, id)
),
summary AS (
    -- Summarise per group
    SELECT
        agent_id,
        COUNT(DISTINCT channel_group) AS distinct_channel_groups,
        ROUND(AVG(channel_avg_requester_id), 2) AS avg_channel_avg_requester_id,
        PERCENTILE_APPROX(next3_requester_id_bucket, 0.5) AS median_next3_requester_id_bucket,
        COUNT(*) FILTER (WHERE agent_tier = 'T3') AS n_agent_tier_t3
    FROM metrics
    GROUP BY agent_id
    HAVING MAX(requester_id_share_of_channel_group) > 0.4
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY avg_channel_avg_requester_id ASC, agent_id), 3) AS avg_channel_avg_requester_id_cume_dist,
        ROW_NUMBER() OVER (ORDER BY avg_channel_avg_requester_id ASC, agent_id) AS avg_channel_avg_requester_id_seq,
        MAX(avg_channel_avg_requester_id) OVER w_distinct_channel_groups_agen AS distinct_channel_groups_agent_id_max_avg_channel,
        COUNT(*) OVER (PARTITION BY distinct_channel_groups, agent_id) AS rows_in_distinct_channel_groups_agent_id
    FROM summary
    WINDOW w_distinct_channel_groups_agen AS (PARTITION BY distinct_channel_groups, agent_id)
)
-- Final projection
SELECT
    agent_id,
    distinct_channel_groups,
    avg_channel_avg_requester_id,
    avg_channel_avg_requester_id_cume_dist,
    avg_channel_avg_requester_id_seq,
    distinct_channel_groups_agent_id_max_avg_channel,
    rows_in_distinct_channel_groups_agent_id
FROM sequenced
WHERE n_agent_tier_t3 >= 4
ORDER BY distinct_channel_groups DESC, rows_in_distinct_channel_groups_agent_id DESC NULLS FIRST
