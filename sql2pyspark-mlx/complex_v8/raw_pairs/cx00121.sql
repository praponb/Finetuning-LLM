WITH recent_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        priority,
        channel,
        tags,
        HOUR(opened_at) AS opened_at_hour,
        CONCAT_WS(': ', channel, priority) AS channel_priority_label
    FROM tickets
    WHERE closed_at < TIMESTAMP '2025-03-09 04:00:00'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(opened_at_hour) IGNORE NULLS OVER (ORDER BY opened_at_hour, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_opened_at_hour,
        MIN(opened_at_hour) OVER (PARTITION BY priority, channel) AS priority_channel_min_opened_at_hour,
        opened_at_hour - LAG(opened_at_hour) OVER w_channel_opened_at_hour_by_op AS opened_at_hour_change,
        ROUND(opened_at_hour - AVG(opened_at_hour) OVER (PARTITION BY agent_id, channel), 2) AS opened_at_hour_vs_agent_id_channel_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY channel, opened_at_hour ORDER BY opened_at_hour DESC NULLS FIRST), 3) AS opened_at_hour_percent_rank,
        SUM(opened_at_hour) OVER (PARTITION BY opened_at_hour, priority ORDER BY opened_at_hour, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS opened_at_hour_sum_1_prior,
        COUNT(*) OVER w_agent_id_channel AS rows_in_agent_id_channel,
        SIZE(COLLECT_SET(priority) OVER w_agent_id_channel) AS distinct_priority_in_agent_id_channel,
        SUM(opened_at_hour) OVER (PARTITION BY channel, opened_at_hour ORDER BY opened_at_hour DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS opened_at_hour_sum_2,
        DENSE_RANK() OVER (PARTITION BY opened_at_hour, priority ORDER BY opened_at_hour) AS opened_at_hour_dense_rank,
        LAG(opened_at_hour, 2, 0) OVER (PARTITION BY channel, opened_at_hour ORDER BY opened_at_hour DESC NULLS FIRST, id) AS prev2_opened_at_hour,
        NTH_VALUE(opened_at_hour, 2) OVER (PARTITION BY channel, opened_at_hour ORDER BY opened_at_hour DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_at_hour,
        LAST_VALUE(opened_at_hour) OVER (PARTITION BY priority, opened_at_hour ORDER BY opened_at_hour, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_opened_at_hour
    FROM recent_tickets
    WINDOW w_channel_opened_at_hour_by_op AS (PARTITION BY channel, opened_at_hour ORDER BY opened_at_hour DESC NULLS FIRST, id), w_agent_id_channel AS (PARTITION BY agent_id, channel)
),
first_rows AS (
    -- First row per group
    SELECT *
    FROM metrics
    WHERE opened_at_hour_dense_rank = 1
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        agent_id,
        priority,
        channel,
        opened_at_hour,
        channel_priority_label,
        first_known_opened_at_hour,
        priority_channel_min_opened_at_hour,
        opened_at_hour_change,
        opened_at_hour_percent_rank,
        opened_at_hour_sum_1_prior,
        rows_in_agent_id_channel,
        distinct_priority_in_agent_id_channel,
        prev2_opened_at_hour,
        second_opened_at_hour,
        last_opened_at_hour,
        MAX(last_opened_at_hour) OVER (PARTITION BY prev2_opened_at_hour, opened_at_hour ORDER BY priority_channel_min_opened_at_hour, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS last_opened_at_hour_max_1_prior,
        ROUND(rows_in_agent_id_channel - AVG(rows_in_agent_id_channel) OVER (PARTITION BY last_opened_at_hour), 2) AS rows_in_agent_id_channel_vs_last_opened_at_hour_,
        ROUND(opened_at_hour_sum_1_prior / NULLIF(SUM(opened_at_hour_sum_1_prior) OVER (PARTITION BY last_opened_at_hour), 0), 4) AS opened_at_hour_sum_1_prior_share_of_last_opened_,
        COUNT(*) OVER (PARTITION BY last_opened_at_hour) AS rows_in_last_opened_at_hour,
        MIN(opened_at_hour_sum_1_prior) OVER w_last_opened_at_hour AS last_opened_at_hour_min_opened_at_hour_sum_1_pri
    FROM first_rows
    WINDOW w_last_opened_at_hour AS (PARTITION BY last_opened_at_hour)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        last_opened_at_hour,
        ROUND(SUM(first_known_opened_at_hour) / NULLIF(SUM(priority_channel_min_opened_at_hour), 0), 3) AS first_known_opened_at_hour_to_priority_channel_m,
        SUM(CASE WHEN channel = 'chat' THEN opened_at_hour_sum_1_prior ELSE 0 END) AS opened_at_hour_sum_1_prior_channel_chat,
        ROUND(SUM(prev2_opened_at_hour) / NULLIF(SUM(opened_at_hour_percent_rank), 0), 3) AS prev2_opened_at_hour_to_opened_at_hour_percent_r,
        PERCENTILE_APPROX(rows_in_last_opened_at_hour, 0.5) AS median_rows_in_last_opened_at_hour,
        COUNT(DISTINCT agent_id) AS distinct_agent_ids,
        ARRAY_SORT(COLLECT_SET(priority)) AS priority_list
    FROM windowed
    WHERE channel_priority_label IS NOT NULL
    GROUP BY last_opened_at_hour
)
-- Final projection
SELECT
    last_opened_at_hour,
    first_known_opened_at_hour_to_priority_channel_m,
    opened_at_hour_sum_1_prior_channel_chat,
    prev2_opened_at_hour_to_opened_at_hour_percent_r
FROM summary
