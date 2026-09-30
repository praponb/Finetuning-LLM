WITH filtered_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(TRIM(actor)) AS actor_clean
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-13 13:00:00'
),
enriched AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.actor,
        s.actor_clean,
        t.channel AS ticket_channel
    FROM filtered_events s
    JOIN tickets t ON t.id = s.ticket_id
)
SELECT
    id,
    ticket_id,
    actor,
    actor_clean,
    ticket_channel,
    INITCAP(TRIM(actor_clean)) AS actor_clean_clean,
    LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug
FROM enriched
WHERE ticket_id IS NOT NULL
ORDER BY actor_clean_clean, ticket_id
