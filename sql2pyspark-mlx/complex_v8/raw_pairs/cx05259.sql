WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE t.event_type IN ('resolved', 'escalated')) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_event_type_count,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at
    FROM cleaned s
)
SELECT
    COALESCE(actor, 'ALL') AS actor,
    COALESCE(event_type, 'ALL') AS event_type,
    ticket_id,
    COUNT(*) AS cnt,
    MIN(event_event_type_count) AS min_event_event_type_count
FROM with_stats
GROUP BY ROLLUP(actor, event_type, ticket_id)
