WITH valid_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        channel,
        csat,
        tags,
        LENGTH(TRIM(priority)) AS priority_len,
        requester_id + csat AS requester_id_plus_csat
    FROM tickets
    WHERE channel IN ('phone', 'chat')
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        channel,
        priority_len,
        requester_id_plus_csat,
        requester_id_plus_csat - LAG(requester_id_plus_csat) OVER w_agent_id_by_priority_len AS requester_id_plus_csat_change,
        ROUND(priority_len / NULLIF(SUM(priority_len) OVER w_channel, 0), 4) AS priority_len_share_of_channel,
        requester_id_plus_csat - LAG(requester_id_plus_csat) OVER (PARTITION BY csat, agent_id ORDER BY csat DESC NULLS FIRST, id) AS requester_id_plus_csat_change_2,
        COUNT(*) OVER (PARTITION BY channel ORDER BY priority_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(AVG(requester_id_plus_csat) OVER (PARTITION BY csat, agent_id ORDER BY csat DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS requester_id_plus_csat_avg_2_prior,
        NTH_VALUE(priority_len, 2) OVER (PARTITION BY csat, agent_id ORDER BY csat DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_priority_len,
        COUNT(*) OVER w_channel AS rows_in_channel,
        MAX(priority_len) OVER (PARTITION BY channel) AS channel_max_priority_len,
        RANK() OVER (PARTITION BY channel ORDER BY priority_len DESC, id) AS priority_len_rank,
        DENSE_RANK() OVER (PARTITION BY channel ORDER BY priority_len DESC) AS priority_len_dense_rank,
        FIRST_VALUE(requester_id_plus_csat) IGNORE NULLS OVER (PARTITION BY agent_id ORDER BY priority_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_requester_id_plus_csat,
        ROUND(priority_len - AVG(priority_len) OVER (PARTITION BY channel), 2) AS priority_len_vs_channel_avg
    FROM valid_tickets
    WINDOW w_agent_id_by_priority_len AS (PARTITION BY agent_id ORDER BY priority_len, id), w_channel AS (PARTITION BY channel)
),
per_group AS (
    -- Summarise per group
    SELECT
        priority_len_rank,
        channel,
        COUNT(*) AS cnt,
        PERCENTILE_APPROX(priority_len, 0.5) AS median_priority_len,
        ROUND(AVG(first_known_requester_id_plus_csat), 2) AS avg_first_known_requester_id_plus_csat,
        COUNT(DISTINCT channel) AS distinct_channels,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(priority_len_dense_rank) FILTER (WHERE channel NOT IN ('email', 'phone')) AS priority_len_dense_rank_channel_not_in_email_pho
    FROM metrics
    WHERE requester_id_plus_csat_change > -15
    GROUP BY priority_len_rank, channel
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        priority_len_rank,
        channel,
        cnt,
        median_priority_len,
        distinct_ids,
        priority_len_dense_rank_channel_not_in_email_pho,
        ROUND(distinct_channels - AVG(distinct_channels) OVER w_priority_len_rank, 2) AS distinct_channels_vs_priority_len_rank_avg,
        FIRST_VALUE(distinct_ids) OVER (PARTITION BY distinct_channels ORDER BY priority_len_dense_rank_channel_not_in_email_pho DESC NULLS FIRST, priority_len_rank, channel ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_distinct_ids,
        NTILE(10) OVER (PARTITION BY distinct_channels ORDER BY priority_len_dense_rank_channel_not_in_email_pho DESC NULLS FIRST, priority_len_rank, channel) AS priority_len_dense_rank_channel_not_in_email_pho_2
    FROM per_group
    WINDOW w_priority_len_rank AS (PARTITION BY priority_len_rank)
)
SELECT
    priority_len_rank,
    median_priority_len,
    distinct_ids,
    priority_len_dense_rank_channel_not_in_email_pho,
    distinct_channels_vs_priority_len_rank_avg,
    first_distinct_ids,
    priority_len_dense_rank_channel_not_in_email_pho_2,
    CASE WHEN channel IN ('chat', 'phone') THEN 'group_a' WHEN channel = 'web' THEN 'group_b' ELSE 'other' END AS channel_group
FROM with_trends
ORDER BY median_priority_len DESC
