WITH prepared AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        csat,
        DATE_FORMAT(closed_at, 'yyyy-MM-dd HH:00') AS closed_at_label,
        CASE WHEN priority IN ('P3', 'P1') THEN 'group_a' WHEN priority = 'P4' THEN 'group_b' ELSE 'other' END AS priority_group,
        LENGTH(TRIM(channel)) AS channel_len
    FROM tickets
    WHERE SIZE(tags) >= 1
),
priority_group_stats AS (
    -- Avg csat per priority_group
    SELECT priority_group, AVG(csat) AS avg_csat
    FROM prepared
    GROUP BY priority_group
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_csat, ROUND(s.csat - g.avg_csat, 2) AS csat_minus_avg
    FROM prepared s
    JOIN priority_group_stats g ON g.priority_group = s.priority_group
    WHERE s.csat > g.avg_csat
),
with_details AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.closed_at_label,
        s.priority_group,
        s.channel_len,
        s.avg_csat,
        su.team AS agent_team
    FROM outliers s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.team IS NOT NULL
)
-- Final projection
SELECT
    id,
    agent_id,
    closed_at_label,
    priority_group,
    channel_len,
    avg_csat
FROM with_details
WHERE avg_csat <= 4.0
ORDER BY closed_at_label, avg_csat DESC
