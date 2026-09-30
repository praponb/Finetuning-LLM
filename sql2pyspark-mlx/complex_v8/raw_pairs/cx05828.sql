WITH base AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-02-12'
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at,
        CONCAT_WS(' / ', actor, event_type) AS actor_event_type_label
    FROM base
)
SELECT
    COALESCE(actor, 'ALL') AS actor,
    COALESCE(event_type, 'ALL') AS event_type,
    ticket_id,
    GROUPING_ID() AS grouping_level,
    SUM(days_since_event_at) AS total_days_since_event_at,
    PERCENTILE_APPROX(days_since_event_at, 0.5) AS median_days_since_event_at
FROM scored
GROUP BY GROUPING SETS ((actor, event_type, ticket_id), (actor), (event_type))
ORDER BY actor NULLS LAST, event_type NULLS LAST, ticket_id NULLS LAST
