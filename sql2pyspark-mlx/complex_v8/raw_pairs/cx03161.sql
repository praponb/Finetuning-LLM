WITH in_window AS (
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
combined_details_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.priority,
        s.agent_id,
        su.tier AS agent_tier,
        su.agent_name,
        su.team AS agent_team
    FROM in_window s
    JOIN support_agents su ON su.id = s.agent_id AND su.team = 'Onboarding'
),
combined_details AS (
    -- Join support_agents
    SELECT
        s.*,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team_2,
        su.tier AS agent_tier_2
    FROM combined_details_agent s
    JOIN support_agents su ON su.id = s.agent_id
)
-- Subtotals with GROUPING SETS
SELECT
    agent_tier_2,
    agent_team_2,
    ticket_id,
    COUNT(*) AS n_rows,
    ARRAY_SORT(COLLECT_SET(agent_tier_2)) AS agent_tier_2_list,
    COUNT(DISTINCT agent_team) AS distinct_agent_teams
FROM combined_details
GROUP BY GROUPING SETS ((agent_tier_2, agent_team_2, ticket_id), (agent_tier_2))
ORDER BY agent_tier_2 NULLS LAST, agent_team_2 NULLS LAST, ticket_id NULLS LAST
