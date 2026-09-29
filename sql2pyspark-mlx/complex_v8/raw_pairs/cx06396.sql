WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(event_type, '#', CAST(id AS STRING)) AS event_type_ref
    FROM ticket_events
    WHERE actor <> 'system'
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, MAX(event_at) AS last_event_at, COUNT(*) AS event_count
    FROM ticket_events
    GROUP BY ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at,
        COALESCE(t.event_count, 0) AS event_count
    FROM base s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT ticket_id, event_at, event_type, event_count
    FROM profiled
) src
PIVOT (
    COUNT(event_count)
    FOR event_type IN ('assigned', 'reopened', 'escalated', 'created')
)
ORDER BY ticket_id, event_at
