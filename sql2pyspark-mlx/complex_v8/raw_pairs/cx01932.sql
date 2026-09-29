WITH cleaned AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        channel,
        csat,
        tags,
        COALESCE(channel, 'UNKNOWN') AS channel_or_unknown,
        SUBSTRING(TRIM(channel), 1, 3) AS channel_prefix
    FROM tickets
    WHERE priority <> 'P1'
      AND agent_id IS NOT NULL
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        requester_id,
        opened_at,
        channel,
        tags,
        channel_or_unknown,
        SIZE(COLLECT_SET(channel_or_unknown) OVER (PARTITION BY channel)) AS distinct_channel_or_unknown_in_channel,
        FIRST_VALUE(requester_id) OVER (PARTITION BY channel, DATE_TRUNC('MONTH', opened_at) ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_requester_id,
        COUNT(*) OVER w_channel AS rows_in_channel,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(LAG(opened_at) OVER (PARTITION BY channel, DATE_TRUNC('MONTH', opened_at) ORDER BY requester_id, id))) / 60, 1) AS minutes_since_prev_opened_at,
        SUM(csat) OVER (PARTITION BY channel) AS channel_sum_csat,
        FIRST_VALUE(csat) OVER (PARTITION BY channel, DATE_TRUNC('MONTH', opened_at) ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_csat,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY channel)) AS distinct_agent_id_in_channel,
        SUM(requester_id) OVER (PARTITION BY channel) AS channel_sum_requester_id,
        LAST_VALUE(requester_id) IGNORE NULLS OVER (PARTITION BY csat, channel_or_unknown ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_requester_id,
        LEAD(requester_id, 1, 0) OVER w_channel_opened_at_month_by_r_2 AS next_requester_id,
        NTH_VALUE(csat, 2) OVER (PARTITION BY csat, channel_or_unknown ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat,
        LAST_VALUE(opened_at) IGNORE NULLS OVER (PARTITION BY csat, channel_or_unknown ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_opened_at
    FROM cleaned
    WINDOW w_channel AS (PARTITION BY channel), w_channel_opened_at_month_by_r_2 AS (PARTITION BY channel, DATE_TRUNC('MONTH', opened_at) ORDER BY requester_id, id)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        channel,
        PERCENTILE_APPROX(next_requester_id, 0.5) AS median_next_requester_id,
        SUM(last_known_requester_id) AS total_last_known_requester_id,
        SUM(channel_sum_csat) AS total_channel_sum_csat
    FROM metrics
    WHERE channel_sum_requester_id > 101
    GROUP BY 1
)
-- Final projection
SELECT
    channel,
    median_next_requester_id,
    total_last_known_requester_id,
    median_next_requester_id - total_channel_sum_csat AS median_next_requester_id_minus_total_channel_sum
FROM summary
WHERE total_channel_sum_csat >= 121
ORDER BY median_next_requester_id DESC
