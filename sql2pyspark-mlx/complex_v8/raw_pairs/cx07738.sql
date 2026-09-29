WITH cleaned_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE priority = 'P2'
      AND ARRAY_CONTAINS(tags, 'outage')
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        channel,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at), channel)) AS distinct_agent_id_in_opened_at_month_channel,
        SUM(csat) OVER (ORDER BY opened_at DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS csat_sum_1_prior,
        MAX(csat) OVER w_opened_at_month_channel AS opened_at_month_channel_max_csat
    FROM cleaned_tickets
    WINDOW w_opened_at_month_channel AS (PARTITION BY DATE_TRUNC('MONTH', opened_at), channel)
),
latest_per_key AS (
    -- Keep the latest row per priority, channel
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        channel,
        distinct_agent_id_in_opened_at_month_channel,
        csat_sum_1_prior,
        opened_at_month_channel_max_csat
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY priority, channel ORDER BY csat_sum_1_prior DESC, id DESC) AS recency_rank
        FROM windowed
    ) ranked
    WHERE recency_rank = 1
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        closed_at + INTERVAL 15 MINUTES AS closed_at_plus_15m,
        CONCAT_WS('-', priority, channel) AS priority_channel_label,
        SUBSTRING(TRIM(priority), 1, 3) AS priority_prefix
    FROM latest_per_key
    WHERE priority = 'P1'
)
SELECT
    COALESCE(priority, 'ALL') AS priority,
    distinct_agent_id_in_opened_at_month_channel,
    GROUPING_ID() AS gid,
    PERCENTILE_APPROX(opened_at_month_channel_max_csat, 0.5) AS median_opened_at_month_channel_max_csat,
    COUNT(*) AS n_rows,
    ROUND(AVG(opened_at_month_channel_max_csat), 2) AS avg_opened_at_month_channel_max_csat
FROM annotated
GROUP BY CUBE(priority, distinct_agent_id_in_opened_at_month_channel)
ORDER BY priority NULLS LAST, distinct_agent_id_in_opened_at_month_channel NULLS LAST
