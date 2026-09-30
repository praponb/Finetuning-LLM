WITH filtered_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        TO_DATE(event_at) AS event_at_date,
        LOWER(TRIM(actor)) AS actor_clean
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-25 09:00:00'
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        event_at_date,
        actor_clean,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM filtered_events
    WHERE actor IN ('system', 'customer')
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, MIN(event_at) AS first_event_at, COUNT(*) AS event_count
    FROM ticket_events
    GROUP BY ticket_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.first_event_at,
        COALESCE(t.event_count, 0) AS event_count
    FROM derived s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
)
SELECT *
FROM (
    SELECT ticket_id, event_type, event_count
    FROM with_stats
) src
PIVOT (
    MIN(event_count) AS min_v, MAX(event_count) AS max_v
    FOR event_type IN ('escalated', 'created', 'reopened')
)
ORDER BY ticket_id
