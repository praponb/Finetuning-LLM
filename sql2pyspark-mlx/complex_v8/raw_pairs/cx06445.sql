WITH scoped AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-02-12 15:00:00'
      AND ticket_id <= 27
),
enriched AS (
    -- Derive helper columns
    SELECT *, DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at
    FROM scoped
),
event_type_stats AS (
    -- Median days_since_event_at per event_type
    SELECT event_type, PERCENTILE_APPROX(days_since_event_at, 0.5) AS median_days_since_event_at
    FROM enriched
    GROUP BY event_type
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_days_since_event_at,
        ROUND(s.days_since_event_at - g.median_days_since_event_at, 2) AS days_since_event_at_minus_median
    FROM enriched s
    JOIN event_type_stats g ON g.event_type = s.event_type
    WHERE s.days_since_event_at >= g.median_days_since_event_at
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    days_since_event_at,
    median_days_since_event_at,
    days_since_event_at_minus_median,
    CONCAT(event_type, '#', CAST(id AS STRING)) AS event_type_ref
FROM above_average
WHERE actor IN ('customer', 'system')
ORDER BY event_type_ref DESC, event_at DESC, id
LIMIT 5
