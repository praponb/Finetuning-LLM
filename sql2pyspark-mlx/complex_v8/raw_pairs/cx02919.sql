WITH cleaned AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
    FROM ticket_events
    WHERE event_type NOT IN ('resolved', 'assigned')
),
enriched_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.actor,
        t.tags AS ticket_tags
    FROM cleaned s
    LEFT JOIN tickets t ON t.id = s.ticket_id AND t.csat > 1
),
scored AS (
    -- Add calculated columns
    SELECT *, SIZE(ticket_tags) AS ticket_tags_count
    FROM enriched_ticket
)
SELECT
    id,
    ticket_id,
    actor,
    ticket_tags_count,
    LENGTH(TRIM(actor)) AS actor_len
FROM scored
ORDER BY ticket_id DESC NULLS FIRST
