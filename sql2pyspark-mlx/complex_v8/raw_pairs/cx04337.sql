WITH recent AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug,
        DATE_TRUNC('WEEK', event_at) AS event_at_week
    FROM ticket_events
    WHERE event_type <> 'replied'
),
enriched_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.event_type_slug,
        s.event_at_week,
        t.csat AS ticket_csat,
        t.closed_at AS ticket_closed_at
    FROM recent s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
combined_details_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type_slug,
        s.event_at_week,
        s.ticket_closed_at,
        t.csat AS ticket_csat_2,
        t.closed_at AS ticket_closed_at_2,
        t.requester_id AS ticket_requester_id
    FROM enriched_ticket s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
with_activity AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM combined_details_ticket s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.actor IS NOT NULL
        )
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM with_activity
    WHERE ticket_csat_2 < (SELECT MAX(ticket_csat_2) FROM with_activity)
),
enriched_ticket_2 AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type_slug,
        s.event_at_week,
        s.ticket_closed_at,
        s.ticket_csat_2,
        s.ticket_closed_at_2,
        s.ticket_requester_id,
        t.channel AS ticket_channel,
        t.csat AS ticket_csat
    FROM without_matches s
    LEFT JOIN tickets t ON t.id = s.ticket_id AND SIZE(t.tags) >= 2
)
-- Final projection
SELECT
    id,
    event_at,
    ticket_closed_at_2,
    ticket_requester_id,
    ticket_csat,
    CONCAT(ticket_channel, '#', CAST(id AS STRING)) AS ticket_channel_ref,
    DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label
FROM enriched_ticket_2
