WITH covered AS (
    -- Events that happened while the ticket was open
    SELECT
        e.id,
        e.ticket_id,
        e.event_at,
        e.event_type,
        t.priority,
        t.agent_id,
        ROUND((UNIX_TIMESTAMP(e.event_at) - UNIX_TIMESTAMP(t.opened_at)) / 3600, 2) AS hours_since_open
    FROM ticket_events e
    JOIN tickets t
      ON t.id = e.ticket_id
     AND e.event_at >= t.opened_at
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 2 DAYS)
),
with_parent_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.priority,
        s.agent_id,
        s.hours_since_open,
        t.requester_id AS ticket_requester_id,
        su.team AS agent_team,
        su.tier AS agent_tier
    FROM covered s
    JOIN tickets t ON t.id = s.ticket_id AND MONTH(t.closed_at) IN (1, 2)
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
with_details AS (
    -- Join support_agents
    SELECT
        s.id,
        s.ticket_id,
        s.priority,
        s.agent_id,
        s.hours_since_open,
        s.ticket_requester_id,
        s.agent_team,
        s.agent_tier,
        su.agent_name,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team_2
    FROM with_parent_ticket s
    JOIN support_agents su ON su.id = s.agent_id
)
SELECT
    agent_team_2,
    agent_team,
    GROUPING_ID() AS grouping_level,
    ROUND(100 * COUNT(CASE WHEN agent_tier = 'T2' THEN 1 END) / COUNT(*), 1) AS pct_agent_tier_t2,
    SUM(ticket_requester_id) AS total_ticket_requester_id
FROM with_details
GROUP BY ROLLUP(agent_team_2, agent_team)
ORDER BY agent_team_2 NULLS LAST, agent_team NULLS LAST
