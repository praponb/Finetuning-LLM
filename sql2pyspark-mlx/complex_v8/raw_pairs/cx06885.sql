WITH filtered_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at,
        DATE_TRUNC('MONTH', event_at) AS event_at_month
    FROM ticket_events
    WHERE actor IN ('system', 'customer')
),
line_level_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.event_at AS event_event_at,
        t.actor AS event_actor
    FROM filtered_events s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
deduped AS (
    -- Keep the latest row per event_type
    SELECT
        id,
        ticket_id,
        event_type,
        event_id,
        event_event_type,
        event_event_at,
        event_actor
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_event_at DESC, id DESC, event_id DESC) AS recency_rank
        FROM line_level_events
    ) ranked
    WHERE recency_rank = 1
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(DATE '2026-01-01', event_event_at) AS days_since_event_event_at
    FROM deduped
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(event_event_type, 'ALL') AS event_event_type,
    ticket_id,
    GROUPING(event_event_type) AS is_total_event_event_type,
    SUM(CASE WHEN event_event_type IN ('replied', 'escalated') THEN days_since_event_event_at ELSE 0 END) AS days_since_event_event_at_event_event_type_in_re
FROM tagged
GROUP BY ROLLUP(event_event_type, ticket_id)
