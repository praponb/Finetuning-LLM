WITH prepared_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
),
tagged AS (
    -- Derive helper columns
    SELECT *, DATE_TRUNC('WEEK', event_at) AS event_at_week
    FROM prepared_events
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.event_at_week,
        t.csat AS ticket_csat,
        t.channel AS ticket_channel
    FROM tagged s
    LEFT JOIN tickets t ON t.id = s.ticket_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_at_week,
    ticket_csat,
    DATE_TRUNC('WEEK', event_at) AS event_at_week_2,
    LENGTH(TRIM(event_type)) AS event_type_len
FROM with_parent
ORDER BY ticket_id
