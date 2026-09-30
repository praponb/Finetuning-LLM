WITH prepared_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
        CASE WHEN actor IN ('customer', 'system') THEN 'group_a' WHEN actor = 'agent' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM ticket_events
    WHERE actor <> 'customer'
),
combined_details_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_type_or_unknown,
        s.actor_group,
        t.closed_at AS ticket_closed_at,
        t.requester_id AS ticket_requester_id,
        t.priority AS ticket_priority
    FROM prepared_events s
    JOIN tickets t ON t.id = s.ticket_id
)
-- Subtotals with GROUPING SETS
SELECT
    ticket_priority,
    ticket_id,
    GROUPING_ID() AS grouping_level,
    MAX(ticket_requester_id) AS max_ticket_requester_id
FROM combined_details_ticket
GROUP BY GROUPING SETS ((ticket_priority, ticket_id), (ticket_priority))
ORDER BY ticket_priority NULLS LAST, ticket_id NULLS LAST
