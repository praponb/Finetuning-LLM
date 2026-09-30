WITH base_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        TO_DATE(event_at) AS event_at_date
    FROM ticket_events
    WHERE event_type IN ('escalated', 'created', 'resolved')
),
enriched AS (
    -- Add calculated columns
    SELECT *, DATE_FORMAT(event_at, 'yyyy-MM-dd') AS event_at_label
    FROM base_events
),
with_children AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.event_type AS event_event_type
    FROM enriched s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
with_details_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_id,
        s.event_event_at,
        s.event_event_type,
        t.requester_id AS ticket_requester_id,
        t.csat AS ticket_csat
    FROM with_children s
    LEFT JOIN tickets t ON t.id = s.ticket_id
)
SELECT id, event_id, metric, amount
FROM (
    SELECT id, event_id, ticket_requester_id, ticket_csat
    FROM with_details_ticket
) src
UNPIVOT (
    amount FOR metric IN (ticket_requester_id, ticket_csat)
) unp
ORDER BY id, event_id, metric
