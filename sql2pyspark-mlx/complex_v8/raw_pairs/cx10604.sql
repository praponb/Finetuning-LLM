WITH scoped AS (
    -- Scope the tickets rows
    SELECT *
    FROM tickets
    WHERE (HOUR(opened_at) BETWEEN 8 AND 12 OR requester_id <= 18)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        agent_id,
        csat,
        channel,
        ROUND(100 * COUNT(CASE WHEN priority IN ('P4', 'P2') THEN 1 END) / COUNT(*), 1) AS pct_priority_in_p4_p2,
        ARRAY_SORT(COLLECT_SET(channel)) AS channel_list,
        COUNT(DISTINCT channel) AS distinct_channels
    FROM scoped
    GROUP BY 1, 2, 3
    HAVING AVG(csat) > 2
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(csat) OVER (PARTITION BY distinct_channels, agent_id) AS distinct_channels_agent_id_max_csat,
        csat - LAG(csat) OVER w_csat_by_pct_priority_in_p4_p AS csat_change,
        LEAD(pct_priority_in_p4_p2) OVER (PARTITION BY csat ORDER BY pct_priority_in_p4_p2 ASC, agent_id, csat, channel) AS next_pct_priority_in_p4_p2,
        pct_priority_in_p4_p2 - LAG(pct_priority_in_p4_p2) OVER (ORDER BY pct_priority_in_p4_p2 DESC, agent_id, csat, channel) AS pct_priority_in_p4_p2_change
    FROM stats
    WINDOW w_csat_by_pct_priority_in_p4_p AS (PARTITION BY csat ORDER BY pct_priority_in_p4_p2 ASC, agent_id, csat, channel)
)
SELECT
    agent_id,
    channel,
    pct_priority_in_p4_p2,
    channel_list,
    distinct_channels
FROM with_trends
WHERE csat > 1
ORDER BY agent_id DESC NULLS FIRST
