WITH cleaned AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        UPPER(TRIM(event_type)) AS event_type_clean,
        CONCAT_WS('-', event_type, actor) AS event_type_actor_label
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-02-12 12:00:00'
      AND event_type IN ('escalated', 'assigned', 'replied')
),
combined_details_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.event_type_clean,
        s.event_type_actor_label,
        t.closed_at AS ticket_closed_at,
        t.opened_at AS ticket_opened_at,
        t.channel AS ticket_channel
    FROM cleaned s
    JOIN tickets t ON t.id = s.ticket_id
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        event_at,
        ticket_closed_at,
        ticket_opened_at,
        ticket_channel,
        CONCAT(ticket_channel, '#', CAST(id AS STRING)) AS ticket_channel_ref,
        TO_DATE(ticket_closed_at) AS ticket_closed_at_date
    FROM combined_details_ticket
)
SELECT
    id,
    event_at,
    ticket_closed_at,
    ticket_opened_at,
    ticket_channel,
    ticket_channel_ref,
    ticket_closed_at_date
FROM labeled
WHERE ticket_channel_ref IS NOT NULL
ORDER BY ticket_closed_at DESC NULLS FIRST
