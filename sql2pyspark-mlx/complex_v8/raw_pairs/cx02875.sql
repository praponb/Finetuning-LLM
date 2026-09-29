WITH scoped_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'MMM yyyy') AS event_at_label,
        event_at + INTERVAL 1 DAYS AS event_at_plus_1d,
        DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label_2
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-02-24'
      AND actor = 'system'
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_at_label,
        event_at_plus_1d,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(event_at_plus_1d)) / 3600, 1) AS hours_event_at_plus_1d_to_event_at,
        DATE_FORMAT(event_at, 'MMM yyyy') AS event_at_label_3,
        HOUR(event_at) AS event_at_hour
    FROM scoped_events
),
combined_details_ticket AS (
    -- Enrich with dimension data
    SELECT s.*, t.opened_at AS ticket_opened_at, t.channel AS ticket_channel
    FROM derived s
    JOIN tickets t ON t.id = s.ticket_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT ticket_id, ticket_channel, hours_event_at_plus_1d_to_event_at
    FROM combined_details_ticket
) src
PIVOT (
    SUM(hours_event_at_plus_1d_to_event_at)
    FOR ticket_channel IN ('email', 'web', 'phone')
)
ORDER BY ticket_id
