WITH base_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        INITCAP(TRIM(event_type)) AS event_type_clean
    FROM ticket_events
    WHERE event_type IS NOT NULL
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.event_type_clean,
        t.requester_id AS ticket_requester_id,
        t.closed_at AS ticket_closed_at,
        t.priority AS ticket_priority,
        su.team AS agent_team,
        su.agent_name
    FROM base_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
)
SELECT *
FROM (
    SELECT ticket_closed_at, ticket_id, event_type, ticket_requester_id
    FROM with_parent
) src
PIVOT (
    SUM(ticket_requester_id)
    FOR event_type IN ('reopened', 'created', 'resolved')
)
