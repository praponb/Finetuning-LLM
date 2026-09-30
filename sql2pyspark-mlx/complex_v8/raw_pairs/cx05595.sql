WITH base AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label,
        DAYOFWEEK(event_at) AS event_at_dayofweek
    FROM ticket_events
    WHERE actor NOT IN ('system', 'customer')
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count
    FROM base s
),
combined_details AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.first_event_at,
        s.last_event_at,
        s.event_count,
        t.requester_id AS ticket_requester_id
    FROM profiled s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.requester_id <= 20
)
SELECT id, field, amount
FROM (
    SELECT id, event_count, ticket_requester_id
    FROM combined_details
) src
UNPIVOT (
    amount FOR field IN (event_count, ticket_requester_id)
) unp
ORDER BY id, field
