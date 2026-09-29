WITH scoped AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags,
        SIZE(tags) AS tags_count,
        DATEDIFF(DATE '2026-01-01', opened_at) AS days_since_opened_at,
        LEAST(requester_id, csat) AS least_requester_id_csat
    FROM tickets
),
latest_per_key AS (
    -- Keep the latest row per agent_id, channel
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags,
        tags_count,
        days_since_opened_at,
        least_requester_id_csat
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id, channel ORDER BY csat DESC, id DESC) AS recency_rank
        FROM scoped
    ) ranked
    WHERE recency_rank = 1
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(tags_count) OVER (ORDER BY csat ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS tags_count_avg_7,
        SIZE(COLLECT_SET(priority) OVER w_opened_at_month_channel) AS distinct_priority_in_opened_at_month_channel,
        ROUND(AVG(requester_id) OVER (ORDER BY csat ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS requester_id_avg_3,
        NTH_VALUE(csat, 2) OVER (ORDER BY csat ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat,
        FIRST_VALUE(least_requester_id_csat) OVER (PARTITION BY YEAR(opened_at) ORDER BY tags_count DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_least_requester_id_csat
    FROM latest_per_key
    WINDOW w_opened_at_month_channel AS (PARTITION BY DATE_TRUNC('MONTH', opened_at), channel)
),
channel_stats AS (
    -- Avg days_since_opened_at per channel
    SELECT channel, AVG(days_since_opened_at) AS avg_days_since_opened_at
    FROM metrics
    GROUP BY channel
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_days_since_opened_at,
        ROUND(s.days_since_opened_at - g.avg_days_since_opened_at, 2) AS days_since_opened_at_minus_avg
    FROM metrics s
    JOIN channel_stats g ON g.channel = s.channel
    WHERE s.days_since_opened_at > g.avg_days_since_opened_at
),
metrics_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY csat, DATE_TRUNC('MONTH', opened_at) ORDER BY first_least_requester_id_csat DESC), 3) AS first_least_requester_id_csat_percent_rank,
        RANK() OVER (PARTITION BY tags_count ORDER BY first_least_requester_id_csat DESC) AS first_least_requester_id_csat_rank,
        RANK() OVER (PARTITION BY csat, DATE_TRUNC('MONTH', opened_at) ORDER BY first_least_requester_id_csat DESC) AS first_least_requester_id_csat_rank_2,
        ROUND(csat - AVG(csat) OVER (PARTITION BY tags_count), 2) AS csat_vs_tags_count_avg
    FROM outliers
),
unique_rows AS (
    -- Keep the latest row per priority
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags,
        tags_count,
        days_since_opened_at,
        least_requester_id_csat,
        tags_count_avg_7,
        distinct_priority_in_opened_at_month_channel,
        requester_id_avg_3,
        second_csat,
        first_least_requester_id_csat,
        avg_days_since_opened_at,
        days_since_opened_at_minus_avg,
        first_least_requester_id_csat_percent_rank,
        first_least_requester_id_csat_rank,
        first_least_requester_id_csat_rank_2,
        csat_vs_tags_count_avg
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY priority ORDER BY least_requester_id_csat DESC, id DESC) AS recency_rank
        FROM metrics_2
    ) ranked
    WHERE recency_rank = 1
)
SELECT DISTINCT tags_count, avg_days_since_opened_at, tags_count_avg_7
FROM unique_rows
WHERE first_least_requester_id_csat_percent_rank < 0.8
ORDER BY avg_days_since_opened_at, tags_count_avg_7
