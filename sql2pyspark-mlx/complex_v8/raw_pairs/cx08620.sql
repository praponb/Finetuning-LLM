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
        tags
    FROM tickets
    WHERE (ARRAY_CONTAINS(tags, 'outage') OR priority = 'P1')
),
with_details AS (
    -- Join support_agents
    SELECT s.*, su.hired_on AS agent_hired_on
    FROM filtered s
    JOIN support_agents su ON su.id = s.agent_id
),
channel_stats AS (
    -- Avg csat per channel
    SELECT channel, AVG(csat) AS avg_csat
    FROM with_details
    GROUP BY channel
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_csat, ROUND(s.csat - g.avg_csat, 2) AS csat_minus_avg
    FROM with_details s
    JOIN channel_stats g ON g.channel = s.channel
    WHERE s.csat > g.avg_csat
),
qualified AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM vs_group s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.event_type = 'reopened'
        )
      AND s.csat > 1
),
combined_details_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.tags,
        s.agent_hired_on,
        s.avg_csat,
        s.csat_minus_avg,
        su.team AS agent_team
    FROM qualified s
    LEFT JOIN support_agents su ON su.id = s.agent_id AND su.team IN ('Tech', 'VIP', 'Onboarding')
)
-- Subtotals with GROUPING SETS
SELECT
    agent_team,
    channel,
    COUNT(DISTINCT priority) AS distinct_prioritys,
    ARRAY_SORT(COLLECT_SET(channel)) AS channel_list,
    ROUND(SUM(requester_id) / NULLIF(SUM(csat_minus_avg), 0), 3) AS requester_id_to_csat_minus_avg_ratio
FROM combined_details_agent
GROUP BY GROUPING SETS ((agent_team, channel), (agent_team), (channel))
ORDER BY agent_team NULLS LAST, channel NULLS LAST
