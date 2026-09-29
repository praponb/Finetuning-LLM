WITH recent_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('DAY', event_at) AS event_at_day,
        CONCAT(event_type, '#', CAST(ticket_id AS STRING)) AS event_type_ref,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug
    FROM ticket_events
    WHERE actor IN ('customer', 'agent')
      AND HOUR(event_at) BETWEEN 10 AND 14
),
with_activity AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM recent_events s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND TO_DATE(t.opened_at) < DATE '2025-02-12'
        )
),
joined_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.tags AS ticket_tags
    FROM with_activity s
    JOIN tickets t ON t.id = s.ticket_id AND t.channel IN ('chat', 'email')
    WHERE ARRAY_CONTAINS(t.tags, 'upgrade')
),
enriched AS (
    -- Add calculated columns
    SELECT *, DAYOFWEEK(event_at) AS event_at_dayofweek
    FROM joined_ticket
    WHERE SIZE(ticket_tags) >= 2
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    ticket_tags,
    event_at_dayofweek,
    SUBSTRING(TRIM(event_type), 1, 4) AS event_type_prefix,
    SUBSTRING(TRIM(actor), 1, 2) AS actor_prefix
FROM enriched
ORDER BY event_at, actor
