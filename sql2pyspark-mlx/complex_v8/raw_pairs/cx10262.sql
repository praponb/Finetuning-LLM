WITH cleaned_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(actor), 1, 2) AS actor_prefix,
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM ticket_events
    WHERE (event_at >= TIMESTAMP '2025-01-25 13:00:00' OR actor NOT IN ('agent', 'system'))
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MAX(event_at) AS last_event_at,
        MAX(event_at) AS last_event_at_2,
        COUNT(*) AS event_count
    FROM ticket_events
    GROUP BY ticket_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at,
        t.last_event_at_2,
        COALESCE(t.event_count, 0) AS event_count
    FROM cleaned_events s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
)
SELECT
    ticket_id,
    actor_or_unknown,
    COUNT(*) FILTER (WHERE event_type IS NOT NULL) AS n_event_type_is_not_null
FROM with_stats
GROUP BY CUBE(ticket_id, actor_or_unknown)
ORDER BY ticket_id NULLS LAST, actor_or_unknown NULLS LAST
