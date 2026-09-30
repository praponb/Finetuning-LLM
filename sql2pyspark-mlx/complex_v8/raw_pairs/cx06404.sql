WITH valid_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        tags,
        csat + requester_id AS csat_plus_requester_id
    FROM tickets
    WHERE channel IN ('phone', 'chat', 'web')
      AND SIZE(tags) >= 1
),
channel_stats AS (
    -- Avg csat_plus_requester_id per channel
    SELECT channel, AVG(csat_plus_requester_id) AS avg_csat_plus_requester_id
    FROM valid_tickets
    GROUP BY channel
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_csat_plus_requester_id,
        ROUND(s.csat_plus_requester_id - g.avg_csat_plus_requester_id, 2) AS csat_plus_requester_id_minus_avg
    FROM valid_tickets s
    JOIN channel_stats g ON g.channel = s.channel
    WHERE s.csat_plus_requester_id > g.avg_csat_plus_requester_id
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM vs_group
    WHERE avg_csat_plus_requester_id < (SELECT MAX(avg_csat_plus_requester_id) FROM vs_group)
)
SELECT
    agent_id,
    priority,
    GROUPING(agent_id) AS is_total_agent_id,
    ROUND(100 * COUNT(CASE WHEN priority <> 'P3' THEN 1 END) / COUNT(*), 1) AS pct_priority_p3
FROM with_activity
GROUP BY GROUPING SETS ((agent_id, priority), (agent_id), ())
ORDER BY agent_id NULLS LAST, priority NULLS LAST
