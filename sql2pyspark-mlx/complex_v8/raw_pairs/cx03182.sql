WITH recent_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        tags
    FROM tickets
    WHERE SIZE(tags) >= 2
      AND opened_at < TIMESTAMP '2025-02-18 14:00:00'
),
enriched AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.channel,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on,
        su.agent_name
    FROM recent_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        agent_team,
        agent_hired_on,
        agent_name,
        ROUND(requester_id - AVG(requester_id) OVER w_agent_id_agent_team, 2) AS requester_id_vs_agent_id_agent_team_avg,
        NTH_VALUE(agent_hired_on, 2) OVER (PARTITION BY YEAR(opened_at) ORDER BY agent_hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_agent_hired_on,
        NTH_VALUE(requester_id, 2) OVER (PARTITION BY YEAR(opened_at) ORDER BY agent_hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id
    FROM enriched
    WINDOW w_agent_id_agent_team AS (PARTITION BY agent_id, agent_team)
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.id) AS event_count,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS last_event_at,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS first_event_at
    FROM with_windows s
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.agent_team,
        s.agent_hired_on,
        s.agent_name,
        s.requester_id_vs_agent_id_agent_team_avg,
        s.second_requester_id,
        s.event_count,
        s.first_event_at,
        t.id AS event_id,
        t.event_at,
        t.event_type,
        t.actor AS event_actor
    FROM summarized_rows s
    JOIN ticket_events t ON t.ticket_id = s.id AND t.event_type <> 'resolved'
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT agent_team, event_actor, event_type, second_requester_id
    FROM with_children_events
) src
PIVOT (
    MAX(second_requester_id)
    FOR event_type IN ('created', 'replied', 'assigned')
)
