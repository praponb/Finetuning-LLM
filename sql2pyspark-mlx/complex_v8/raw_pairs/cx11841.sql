WITH prepared AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        CASE WHEN requester_id >= 21 THEN 'A' WHEN requester_id >= 11 THEN 'B' ELSE 'C' END AS requester_id_tier,
        LENGTH(TRIM(channel)) AS channel_len,
        LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug
    FROM tickets
),
stats AS (
    -- Group-level totals
    SELECT
        priority,
        channel,
        channel_len,
        SUM(csat) AS total_csat,
        COUNT(*) FILTER (WHERE priority = 'P2') AS n_priority_p2,
        PERCENTILE_APPROX(channel_len, 0.5) AS median_channel_len
    FROM prepared
    GROUP BY priority, channel, channel_len
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER w_n_priority_p2_by_median_chan AS rn,
        ROW_NUMBER() OVER w_all_by_median_channel_len AS median_channel_len_seq,
        ROUND(total_csat / NULLIF(SUM(total_csat) OVER w_channel_len, 0), 4) AS total_csat_share_of_channel_len,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY channel_len)) AS distinct_priority_in_channel_len,
        LAST_VALUE(channel_len) IGNORE NULLS OVER (PARTITION BY median_channel_len, channel_len ORDER BY median_channel_len, priority, channel, channel_len ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel_len
    FROM stats
    WINDOW w_n_priority_p2_by_median_chan AS (PARTITION BY n_priority_p2 ORDER BY median_channel_len, priority, channel, channel_len), w_all_by_median_channel_len AS (ORDER BY median_channel_len NULLS LAST, priority, channel, channel_len), w_channel_len AS (PARTITION BY channel_len)
)
-- Final projection
SELECT
    priority,
    channel,
    channel_len,
    total_csat,
    n_priority_p2,
    median_channel_len,
    rn,
    median_channel_len_seq,
    total_csat_share_of_channel_len,
    distinct_priority_in_channel_len,
    LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug
FROM sequenced
WHERE n_priority_p2 > 2
ORDER BY channel_slug DESC
