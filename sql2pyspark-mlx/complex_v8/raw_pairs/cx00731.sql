WITH base_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug
    FROM ticket_events
    WHERE ticket_id <= 43
      AND event_at >= TIMESTAMP '2025-01-31 13:00:00'
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at,
        (SELECT COUNT(*) FILTER (WHERE t.actor = 'customer') FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_actor_count
    FROM base_events s
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.actor_slug,
        s.last_event_at,
        s.event_actor_count,
        t.priority AS ticket_priority,
        t.opened_at AS ticket_opened_at
    FROM profiled s
    LEFT JOIN tickets t ON t.id = s.ticket_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    actor,
    actor_slug,
    last_event_at,
    event_actor_count,
    ticket_priority,
    ticket_opened_at
FROM combined_details
ORDER BY ticket_priority, id
LIMIT 10
