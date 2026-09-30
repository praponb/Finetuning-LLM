WITH valid AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        channel,
        csat,
        tags,
        LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug
    FROM tickets
    WHERE TO_DATE(closed_at) < DATE '2025-02-22'
),
with_parent_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.channel,
        s.csat,
        s.tags,
        s.channel_slug,
        su.agent_name
    FROM valid s
    LEFT JOIN support_agents su ON su.id = s.agent_id AND su.tier IN ('T1', 'T2')
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_all_by_csat_ties, 3) AS csat_percent_rank,
        MAX(csat) OVER (ORDER BY csat DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS csat_max_3_prior,
        SUM(csat) OVER (PARTITION BY channel ORDER BY csat ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS csat_sum_2,
        ROUND(csat - AVG(csat) OVER (PARTITION BY channel, DATE_TRUNC('MONTH', opened_at)), 2) AS csat_vs_channel_opened_at_month_avg,
        MIN(requester_id) OVER (PARTITION BY channel, DATE_TRUNC('MONTH', opened_at)) AS channel_opened_at_month_min_requester_id,
        LAST_VALUE(requester_id) OVER (ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_requester_id,
        ROUND(PERCENT_RANK() OVER w_all_by_csat, 3) AS csat_percent_rank_2,
        COUNT(*) OVER (PARTITION BY channel, DATE_TRUNC('MONTH', opened_at)) AS rows_in_channel_opened_at_month,
        NTH_VALUE(opened_at, 2) OVER (PARTITION BY channel ORDER BY csat ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_at,
        RANK() OVER (ORDER BY csat DESC, id) AS csat_rank,
        ROW_NUMBER() OVER (PARTITION BY csat ORDER BY csat DESC, id) AS seq_no,
        ROUND(AVG(csat) OVER (ORDER BY csat DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS csat_avg_1_prior,
        LAST_VALUE(csat) IGNORE NULLS OVER (ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_csat,
        SUM(requester_id) OVER (PARTITION BY channel, DATE_TRUNC('MONTH', opened_at)) AS channel_opened_at_month_sum_requester_id,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY channel, DATE_TRUNC('MONTH', opened_at))) AS distinct_agent_id_in_channel_opened_at_month
    FROM with_parent_agent
    WINDOW w_all_by_csat_ties AS (ORDER BY csat DESC), w_all_by_csat AS (ORDER BY csat DESC, id), w_channel_opened_at_month AS (PARTITION BY channel, DATE_TRUNC('MONTH', opened_at))
),
leaders AS (
    -- Top 2 per group
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        channel,
        csat,
        tags,
        channel_slug,
        agent_name,
        csat_percent_rank,
        csat_max_3_prior,
        csat_sum_2,
        csat_vs_channel_opened_at_month_avg,
        channel_opened_at_month_min_requester_id,
        last_requester_id,
        csat_percent_rank_2,
        rows_in_channel_opened_at_month,
        second_opened_at,
        seq_no,
        csat_avg_1_prior,
        last_known_csat,
        channel_opened_at_month_sum_requester_id,
        distinct_agent_id_in_channel_opened_at_month
    FROM with_trends
    WHERE csat_rank <= 2
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY last_known_csat, distinct_agent_id_in_channel_opened_at_month) AS rows_in_last_known_csat_distinct_agent_id_in_cha,
        LAST_VALUE(csat_percent_rank) OVER (PARTITION BY distinct_agent_id_in_channel_opened_at_month ORDER BY csat_max_3_prior DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_csat_percent_rank,
        LEAD(opened_at) OVER w_distinct_agent_id_in_channel_2 AS next_opened_at,
        SUM(seq_no) OVER (ORDER BY csat_percent_rank_2 DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS seq_no_sum_4,
        MAX(requester_id) OVER (PARTITION BY distinct_agent_id_in_channel_opened_at_month ORDER BY csat_max_3_prior DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS requester_id_max_2,
        ROW_NUMBER() OVER w_agent_id_last_known_csat_by AS rn
    FROM leaders
    WINDOW w_distinct_agent_id_in_channel_2 AS (PARTITION BY distinct_agent_id_in_channel_opened_at_month ORDER BY csat_max_3_prior DESC, id), w_agent_id_last_known_csat_by AS (PARTITION BY agent_id, last_known_csat ORDER BY csat_sum_2 ASC, id)
)
-- Final projection
SELECT
    id,
    agent_id,
    channel,
    tags,
    channel_slug,
    agent_name,
    csat_percent_rank,
    csat_max_3_prior,
    last_requester_id,
    csat_percent_rank_2,
    rows_in_channel_opened_at_month,
    second_opened_at,
    seq_no,
    csat_avg_1_prior,
    last_known_csat,
    distinct_agent_id_in_channel_opened_at_month,
    rows_in_last_known_csat_distinct_agent_id_in_cha,
    last_csat_percent_rank,
    next_opened_at,
    seq_no_sum_4,
    requester_id_max_2,
    SIZE(tags) AS tags_count
FROM windowed
ORDER BY seq_no, last_csat_percent_rank DESC
