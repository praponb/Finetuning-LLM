WITH base AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        SUBSTRING(TRIM(priority), 1, 2) AS priority_prefix,
        CONCAT_WS(' / ', channel, priority) AS channel_priority_label
    FROM tickets
    WHERE SIZE(tags) >= 2
),
summary AS (
    -- Group-level totals
    SELECT
        priority,
        channel,
        COUNT(*) AS cnt,
        MIN(csat) AS min_csat,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(100 * COUNT(CASE WHEN priority = 'P2' THEN 1 END) / COUNT(*), 1) AS pct_priority_p2,
        COUNT(DISTINCT channel) AS distinct_channels
    FROM base
    GROUP BY priority, channel
    HAVING SUM(csat) > 2
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY distinct_ids) AS rows_in_distinct_ids,
        ROUND(AVG(distinct_channels) OVER w_distinct_ids, 2) AS distinct_ids_avg_distinct_channels,
        ROUND(PERCENT_RANK() OVER (PARTITION BY priority, distinct_channels ORDER BY cnt), 3) AS cnt_percent_rank,
        SUM(distinct_channels) OVER (PARTITION BY priority, distinct_channels ORDER BY cnt, priority, channel ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_distinct_channels
    FROM summary
    WINDOW w_distinct_ids AS (PARTITION BY distinct_ids)
)
-- Final projection
SELECT
    priority,
    channel,
    cnt,
    min_csat,
    distinct_ids,
    pct_priority_p2,
    distinct_channels,
    rows_in_distinct_ids,
    distinct_ids_avg_distinct_channels,
    cnt_percent_rank,
    running_distinct_channels,
    distinct_ids + rows_in_distinct_ids AS distinct_ids_plus_rows_in_distinct_ids,
    LEAST(distinct_channels, rows_in_distinct_ids) AS least_distinct_channels_rows_in_distinct_ids
FROM with_trends
ORDER BY pct_priority_p2 DESC
