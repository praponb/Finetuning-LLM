WITH filtered AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE actor IS NOT NULL
),
exploded_detail_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.days_since_event_at,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.event_at AS event_event_at,
        t.actor AS event_actor
    FROM filtered s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.actor NOT IN ('agent', 'customer')
),
stats AS (
    -- Summarise per group
    SELECT
        TO_DATE(event_event_at) AS event_event_at_day,
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        MIN(days_since_event_at) AS min_days_since_event_at,
        ROUND(AVG(days_since_event_at), 2) AS avg_days_since_event_at,
        COUNT(*) AS row_count,
        SUM(CASE WHEN event_event_type IN ('resolved', 'assigned', 'reopened') THEN 1 ELSE 0 END) AS n_event_event_type_in_resolved,
        ROUND(100 * COUNT(CASE WHEN event_actor IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_event_actor_is_not_null
    FROM exploded_detail_events
    GROUP BY TO_DATE(event_event_at), DATE_TRUNC('MONTH', event_at)
    HAVING COUNT(*) >= 3
)
-- Final projection
SELECT
    event_event_at_day,
    row_count,
    n_event_event_type_in_resolved,
    FLOOR(avg_days_since_event_at / 100) * 100 AS avg_days_since_event_at_bucket
FROM stats
WHERE min_days_since_event_at <= 560
ORDER BY row_count DESC, avg_days_since_event_at_bucket NULLS LAST
