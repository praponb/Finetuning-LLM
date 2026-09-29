WITH filtered AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        channel,
        tags,
        SIZE(tags) AS tags_count,
        DATEDIFF(DATE '2026-01-01', opened_at) AS days_since_opened_at
    FROM tickets
    WHERE NOT ARRAY_CONTAINS(tags, 'refund')
),
channel_stats AS (
    -- Median requester_id per channel
    SELECT channel, PERCENTILE_APPROX(requester_id, 0.5) AS median_requester_id
    FROM filtered
    GROUP BY channel
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_requester_id,
        ROUND(s.requester_id - g.median_requester_id, 2) AS requester_id_minus_median
    FROM filtered s
    JOIN channel_stats g ON g.channel = s.channel
    WHERE s.requester_id >= g.median_requester_id
)
SELECT
    COALESCE(channel, 'ALL') AS channel,
    agent_id,
    GROUPING_ID() AS gid,
    SUM(CASE WHEN channel = 'chat' THEN 1 ELSE 0 END) AS n_channel_chat,
    COUNT(DISTINCT channel) AS distinct_channels
FROM compared
GROUP BY ROLLUP(channel, agent_id)
ORDER BY channel NULLS LAST, agent_id NULLS LAST
