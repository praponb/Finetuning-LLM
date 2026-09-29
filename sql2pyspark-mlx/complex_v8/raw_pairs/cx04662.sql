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
        SUBSTRING(TRIM(priority), 1, 2) AS priority_prefix,
        LENGTH(TRIM(priority)) AS priority_len
    FROM tickets
    WHERE priority IS NOT NULL
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        channel,
        csat,
        priority,
        ARRAY_SORT(COLLECT_SET(channel)) AS channel_list,
        COUNT(*) AS n_rows,
        PERCENTILE_APPROX(priority_len, 0.5) AS median_priority_len
    FROM scoped
    WHERE priority_len BETWEEN 5 AND 11
    GROUP BY channel, csat, priority
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(median_priority_len / NULLIF(SUM(median_priority_len) OVER (PARTITION BY csat), 0), 4) AS median_priority_len_share_of_csat,
        ROUND(CUME_DIST() OVER (PARTITION BY csat ORDER BY median_priority_len DESC, channel, csat, priority), 3) AS median_priority_len_cume_dist,
        LAST_VALUE(n_rows) IGNORE NULLS OVER (PARTITION BY csat ORDER BY median_priority_len DESC, channel, csat, priority ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_n_rows,
        ROW_NUMBER() OVER (PARTITION BY csat ORDER BY median_priority_len DESC, channel, csat, priority) AS median_priority_len_seq,
        SIZE(COLLECT_SET(channel) OVER (PARTITION BY csat)) AS distinct_channel_in_csat
    FROM totals
),
leaders AS (
    -- First row per group
    SELECT
        channel,
        csat,
        priority,
        channel_list,
        n_rows,
        median_priority_len,
        median_priority_len_share_of_csat,
        median_priority_len_cume_dist,
        last_known_n_rows,
        distinct_channel_in_csat
    FROM with_trends
    WHERE median_priority_len_seq = 1
),
derived AS (
    -- Derive helper columns
    SELECT *, COALESCE(channel, 'UNKNOWN') AS channel_or_unknown
    FROM leaders
    WHERE SIZE(channel_list) >= 2
)
SELECT
    channel,
    csat,
    median_priority_len_share_of_csat,
    median_priority_len_cume_dist,
    last_known_n_rows,
    channel_or_unknown
FROM derived
ORDER BY csat NULLS LAST
