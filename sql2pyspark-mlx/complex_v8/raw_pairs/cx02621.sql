WITH filtered AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        ABS(csat - 3) AS csat_distance,
        LENGTH(TRIM(channel)) AS channel_len,
        CASE WHEN csat >= 3 THEN 'HIGH' WHEN csat >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS csat_tier
    FROM tickets
    WHERE SIZE(tags) >= 1
      AND agent_id IS NOT NULL
),
with_parent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.channel,
        s.csat,
        s.tags,
        s.csat_distance,
        s.channel_len,
        s.csat_tier,
        su.agent_name,
        su.hired_on AS agent_hired_on
    FROM filtered s
    JOIN support_agents su ON su.id = s.agent_id AND su.tier IN ('T2', 'T1')
)
SELECT
    channel_len,
    csat_distance,
    GROUPING(channel_len) AS is_total_channel_len,
    ROUND(STDDEV(csat), 2) AS stddev_csat,
    ROUND(AVG(channel_len), 2) AS avg_channel_len,
    SUM(channel_len) AS total_channel_len
FROM with_parent
GROUP BY ROLLUP(channel_len, csat_distance)
ORDER BY channel_len NULLS LAST, csat_distance NULLS LAST
