WITH cleaned_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN actor IN ('customer', 'system') THEN 'group_a' WHEN actor = 'agent' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM ticket_events
    WHERE event_type IS NOT NULL
),
enriched_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.actor,
        t.priority AS ticket_priority,
        t.csat AS ticket_csat,
        t.closed_at AS ticket_closed_at,
        su.agent_name
    FROM cleaned_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
stats AS (
    -- Group-level totals
    SELECT
        ticket_id,
        ticket_priority,
        DATE_TRUNC('MONTH', ticket_closed_at) AS ticket_closed_at_month,
        SUM(CASE WHEN LOWER(agent_name) LIKE '%ui%' THEN ticket_csat ELSE 0 END) AS ticket_csat_lower_agent_name_like_ui,
        MAX(ticket_closed_at) AS last_ticket_closed_at
    FROM enriched_ticket
    GROUP BY ticket_id, ticket_priority, DATE_TRUNC('MONTH', ticket_closed_at)
)
SELECT *
FROM (
    SELECT ticket_id, ticket_closed_at_month, ticket_priority, ticket_csat_lower_agent_name_like_ui
    FROM stats
) src
PIVOT (
    SUM(ticket_csat_lower_agent_name_like_ui)
    FOR ticket_priority IN ('P1', 'P3', 'P4', 'P2')
)
ORDER BY ticket_id, ticket_closed_at_month
