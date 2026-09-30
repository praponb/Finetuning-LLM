WITH scoped_tickets AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        channel,
        tags,
        LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug
    FROM tickets
    WHERE requester_id > 7
      AND opened_at >= TIMESTAMP '2025-01-13 09:00:00'
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS last_event_at
    FROM scoped_tickets s
),
joined AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.channel,
        s.tags,
        s.channel_slug,
        su.tier AS agent_tier
    FROM with_stats s
    JOIN support_agents su ON su.id = s.agent_id AND su.tier = 'T2'
),
with_details_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.channel,
        s.tags,
        s.agent_tier,
        su.hired_on AS agent_hired_on,
        su.agent_name,
        su.team AS agent_team
    FROM joined s
    JOIN support_agents su ON su.id = s.agent_id
)
SELECT
    agent_team,
    agent_id,
    agent_tier,
    GROUPING(agent_team) AS is_total_agent_team,
    ROUND(100 * COUNT(CASE WHEN agent_team NOT IN ('Tech', 'Billing') THEN 1 END) / COUNT(*), 1) AS pct_agent_team_not_in_tech_billi
FROM with_details_agent
GROUP BY ROLLUP(agent_team, agent_id, agent_tier)
ORDER BY agent_team NULLS LAST, agent_id NULLS LAST, agent_tier NULLS LAST
