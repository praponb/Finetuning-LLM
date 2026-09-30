WITH prepared AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type = 'resolved'
      AND event_at < TIMESTAMP '2025-02-12 08:00:00'
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        INITCAP(TRIM(actor)) AS actor_clean,
        SUBSTRING(TRIM(event_type), 1, 4) AS event_type_prefix
    FROM prepared
    WHERE ticket_id IS NOT NULL
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        COUNT(*) FILTER (WHERE actor NOT IN ('customer', 'system')) AS event_actor_count,
        COUNT(*) AS event_count,
        COUNT(*) AS event_count_2
    FROM ticket_events
    GROUP BY ticket_id
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_actor_count, 0) AS event_actor_count,
        COALESCE(t.event_count, 0) AS event_count,
        COALESCE(t.event_count_2, 0) AS event_count_2
    FROM enriched s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    actor_clean,
    event_type_prefix,
    event_actor_count,
    event_count,
    event_count_2
FROM with_counts
ORDER BY ticket_id
