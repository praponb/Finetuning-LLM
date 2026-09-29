WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor = 'system'
      AND ticket_id IS NOT NULL
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at
    FROM recent s
),
joined AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.first_event_at,
        t.requester_id AS ticket_requester_id,
        t.tags AS ticket_tags
    FROM profiled s
    LEFT JOIN tickets t ON t.id = s.ticket_id
)
SELECT *
FROM (
    SELECT first_event_at, actor, ticket_requester_id
    FROM joined
) src
PIVOT (
    SUM(ticket_requester_id)
    FOR actor IN ('customer', 'agent')
)
