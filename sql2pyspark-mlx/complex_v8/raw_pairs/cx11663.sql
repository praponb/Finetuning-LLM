WITH recent AS (
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
        CONCAT_WS('-', channel, priority) AS channel_priority_label,
        ROUND(requester_id / NULLIF(csat, 0), 3) AS requester_id_per_csat
    FROM tickets
    WHERE (opened_at < TIMESTAMP '2025-02-24 08:00:00' OR csat <= 4)
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT MIN(t.closed_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS first_ticket_closed_at,
        (SELECT COUNT(CASE WHEN t.priority = 'P1' THEN 1 END) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_priority_count
    FROM recent s
),
joined AS (
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
        s.first_ticket_closed_at,
        s.ticket_priority_count,
        su.team AS agent_team,
        su.tier AS agent_tier
    FROM profiled s
    JOIN support_agents su ON su.id = s.agent_id
)
SELECT
    channel,
    priority,
    ROUND(100 * COUNT(CASE WHEN agent_team IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_agent_team_is_not_null,
    MAX(csat) AS max_csat
FROM joined
GROUP BY GROUPING SETS ((channel, priority), (channel), (priority), ())
