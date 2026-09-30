WITH prepared_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown,
        event_at + INTERVAL 2 HOURS AS event_at_plus_2h,
        LOWER(TRIM(event_type)) AS event_type_clean
    FROM ticket_events
    WHERE ticket_id <= 23
),
with_parent_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor_or_unknown,
        s.event_at_plus_2h,
        s.event_type_clean,
        t.opened_at AS ticket_opened_at
    FROM prepared_events s
    JOIN tickets t ON t.id = s.ticket_id
),
with_details_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor_or_unknown,
        s.event_at_plus_2h,
        s.ticket_opened_at,
        t.closed_at AS ticket_closed_at,
        t.requester_id AS ticket_requester_id,
        t.channel AS ticket_channel
    FROM with_parent_ticket s
    LEFT JOIN tickets t ON t.id = s.ticket_id
)
-- Subtotals with CUBE
SELECT
    ticket_channel,
    event_type,
    GROUPING_ID() AS gid,
    ARRAY_SORT(COLLECT_SET(ticket_channel)) AS ticket_channel_list
FROM with_details_ticket
GROUP BY CUBE(ticket_channel, event_type)
ORDER BY ticket_channel NULLS LAST, event_type NULLS LAST
