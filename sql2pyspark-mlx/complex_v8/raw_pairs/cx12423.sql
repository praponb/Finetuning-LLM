WITH prepared_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor <> 'agent'
),
with_parent AS (
    -- Attach ticket attributes
    SELECT s.*, t.agent_id AS ticket_agent_id
    FROM prepared_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    ticket_agent_id,
    event_at + INTERVAL 30 MINUTES AS event_at_plus_30m
FROM with_parent
ORDER BY ticket_agent_id DESC
