WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE (actor <> 'agent' OR event_type = 'escalated')
),
joined AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.requester_id AS ticket_requester_id,
        su.tier AS agent_tier,
        su.agent_name
    FROM cleaned s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
joined_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.agent_tier,
        t.priority AS ticket_priority,
        t.closed_at AS ticket_closed_at
    FROM joined s
    LEFT JOIN tickets t ON t.id = s.ticket_id AND t.priority NOT IN ('P2', 'P1')
),
with_children AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.ticket_priority,
        s.ticket_closed_at,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.actor AS event_actor
    FROM joined_ticket s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
)
SELECT
    id,
    ticket_id,
    event_at,
    ticket_priority,
    event_id,
    event_event_at,
    event_actor,
    DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at,
    SUBSTRING(TRIM(event_actor), 1, 4) AS event_actor_prefix
FROM with_children
WHERE event_id >= 3
ORDER BY event_actor_prefix DESC, event_id
