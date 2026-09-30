WITH cleaned_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at,
        CONCAT_WS('-', actor, event_type) AS actor_event_type_label
    FROM ticket_events
),
with_details AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.actor_event_type_label,
        t.requester_id AS ticket_requester_id,
        t.priority AS ticket_priority,
        t.opened_at AS ticket_opened_at,
        su.hired_on AS agent_hired_on,
        su.agent_name
    FROM cleaned_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id AND MONTH(t.opened_at) IN (1, 3)
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
with_parent_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.actor_event_type_label,
        s.ticket_requester_id,
        s.ticket_opened_at,
        t.agent_id AS ticket_agent_id
    FROM with_details s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.agent_id IS NOT NULL
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    actor_event_type_label,
    ticket_opened_at,
    ticket_agent_id,
    LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
    DATEDIFF(DATE '2025-06-30', ticket_opened_at) AS days_since_ticket_opened_at
FROM with_parent_ticket
ORDER BY ticket_id DESC, id
LIMIT 20
