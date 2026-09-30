WITH filtered AS (
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
        INITCAP(TRIM(channel)) AS channel_clean,
        CASE WHEN channel IN ('phone', 'chat') THEN 'group_a' WHEN channel = 'email' THEN 'group_b' ELSE 'other' END AS channel_group,
        ABS(requester_id - 13) AS requester_id_distance
    FROM tickets
    WHERE requester_id BETWEEN 2 AND 25
      AND NOT ARRAY_CONTAINS(tags, 'login')
),
channel_group_stats AS (
    -- Avg requester_id_distance per channel_group
    SELECT channel_group, AVG(requester_id_distance) AS avg_requester_id_distance
    FROM filtered
    GROUP BY channel_group
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_requester_id_distance,
        ROUND(s.requester_id_distance - g.avg_requester_id_distance, 2) AS requester_id_distance_minus_avg
    FROM filtered s
    JOIN channel_group_stats g ON g.channel_group = s.channel_group
    WHERE s.requester_id_distance > g.avg_requester_id_distance
),
filtered_2 AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM compared s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND HOUR(t.closed_at) BETWEEN 8 AND 11
        )
)
-- Subtotals with GROUPING SETS
SELECT
    csat,
    COALESCE(channel, 'ALL') AS channel,
    COUNT(DISTINCT channel_group) AS distinct_channel_groups
FROM filtered_2
GROUP BY GROUPING SETS ((csat, channel), (csat), (channel), ())
ORDER BY csat NULLS LAST, channel NULLS LAST
