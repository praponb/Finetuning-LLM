WITH cleaned_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(actor), 1, 3) AS actor_prefix,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE event_type IN ('created', 'assigned')
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        COUNT(*) FILTER (WHERE event_type = 'assigned') AS event_event_type_count,
        MIN(event_at) AS first_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_event_type_count, 0) AS event_event_type_count,
        t.first_event_at
    FROM cleaned_events s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
event_stats_2 AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MAX(event_at) AS last_event_at,
        MIN(event_at) AS first_event_at_2
    FROM ticket_events
    GROUP BY ticket_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at,
        t.first_event_at_2
    FROM summarized_rows s
    LEFT JOIN event_stats_2 t ON t.ticket_id = s.ticket_id
)
SELECT id, metric, metric_value
FROM (
    SELECT id, event_event_type_count, days_since_event_at
    FROM with_stats
) src
UNPIVOT (
    metric_value FOR metric IN (event_event_type_count, days_since_event_at)
) unp
ORDER BY id, metric
