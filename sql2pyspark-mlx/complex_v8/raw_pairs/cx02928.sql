WITH covered AS (
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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 1 DAYS)
),
joined_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.priority,
        s.agent_id,
        s.hours_since_open,
        t.priority AS ticket_priority,
        t.requester_id AS ticket_requester_id,
        t.opened_at AS ticket_opened_at
    FROM covered s
    JOIN tickets t ON t.id = s.ticket_id
),
with_children_tickets AS (
    -- One row per ticket
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.priority,
        s.agent_id,
        s.ticket_priority,
        t.id AS ticket_id_2,
        t.tags AS ticket_tags,
        t.opened_at AS ticket_opened_at_2
    FROM joined_ticket s
    LEFT JOIN tickets t ON t.agent_id = s.agent_id AND ARRAY_CONTAINS(t.tags, 'refund')
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.priority,
        s.agent_id,
        s.ticket_id_2,
        s.ticket_tags,
        s.ticket_opened_at_2,
        t.csat AS ticket_csat,
        t.opened_at AS ticket_opened_at
    FROM with_children_tickets s
    LEFT JOIN tickets t ON t.id = s.ticket_id
)
SELECT *
FROM (
    SELECT event_type, priority, ticket_csat
    FROM combined_details
) src
PIVOT (
    MIN(ticket_csat)
    FOR priority IN ('P2', 'P4', 'P1', 'P3')
)
ORDER BY event_type
