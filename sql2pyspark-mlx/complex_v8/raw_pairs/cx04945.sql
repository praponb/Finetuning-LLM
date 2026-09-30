WITH filtered AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'MMM yyyy') AS event_at_label,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
        LOWER(TRIM(actor)) AS actor_clean
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-03-08'
      AND event_type IS NOT NULL
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at
    FROM filtered s
),
filtered_2 AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM with_stats s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_type IS NOT NULL
              AND t.actor IS NOT NULL
        )
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT event_type, ticket_id, event_type_or_unknown, event_count
    FROM filtered_2
) src
PIVOT (
    COUNT(event_count)
    FOR event_type_or_unknown IN ('UNKNOWN', 'assigned', 'resolved')
)
ORDER BY event_type, ticket_id
