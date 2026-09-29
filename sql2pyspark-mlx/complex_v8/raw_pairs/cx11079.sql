WITH filtered_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(TRIM(event_type)) AS event_type_clean,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at,
        CONCAT_WS(': ', actor, event_type) AS actor_event_type_label
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-02-18 10:00:00'
),
with_parent_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.days_since_event_at,
        s.actor_event_type_label,
        t.csat AS ticket_csat,
        t.requester_id AS ticket_requester_id,
        su.team AS agent_team
    FROM filtered_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_type,
        days_since_event_at,
        ticket_csat,
        agent_team,
        ROUND(days_since_event_at - AVG(days_since_event_at) OVER (PARTITION BY ticket_csat, agent_team), 2) AS days_since_event_at_vs_ticket_csat_agent_team_av,
        COUNT(*) OVER (PARTITION BY ticket_csat, agent_team) AS rows_in_ticket_csat_agent_team
    FROM with_parent_ticket
)
SELECT
    ticket_id,
    ticket_csat,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(CASE WHEN agent_team NOT IN ('Tech', 'Billing') THEN days_since_event_at ELSE 0 END) AS days_since_event_at_agent_team_not_in_tech_billi
FROM windowed
GROUP BY ROLLUP(ticket_id, ticket_csat)
ORDER BY ticket_id NULLS LAST, ticket_csat NULLS LAST
