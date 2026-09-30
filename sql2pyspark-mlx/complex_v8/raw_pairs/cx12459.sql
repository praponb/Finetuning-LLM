WITH filtered_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        channel,
        tags
    FROM tickets
    WHERE requester_id >= 7
),
detail_rows_events AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.channel,
        s.tags,
        t.id AS event_id,
        t.event_type,
        t.event_at
    FROM filtered_tickets s
    JOIN ticket_events t ON t.ticket_id = s.id AND t.event_type <> 'created'
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT event_at, event_type, requester_id
    FROM detail_rows_events
) src
PIVOT (
    AVG(requester_id)
    FOR event_type IN ('escalated', 'reopened', 'resolved', 'assigned')
)
