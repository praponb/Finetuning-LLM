WITH scoped AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('QUARTER', event_at) AS event_at_quarter
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-25 08:00:00'
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        event_at_quarter,
        CONCAT_WS(' / ', actor, event_type) AS actor_event_type_label,
        HOUR(event_at_quarter) AS event_at_quarter_hour,
        SUBSTRING(TRIM(actor), 1, 3) AS actor_prefix
    FROM scoped
    WHERE event_at_quarter >= TIMESTAMP '2025-01-25 09:00:00'
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        COUNT(CASE WHEN event_type = 'created' THEN 1 END) AS event_event_type_count,
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
    FROM tagged s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
tagged_2 AS (
    -- Label and score rows
    SELECT
        *,
        DATE_FORMAT(first_event_at, 'yyyy-MM-dd HH:00') AS first_event_at_label,
        CONCAT_WS(' / ', event_type, actor_event_type_label) AS event_type_actor_event_type_label_label
    FROM summarized_rows
)
-- Subtotals with GROUPING SETS
SELECT
    event_type,
    ticket_id,
    GROUPING(event_type) AS is_total_event_type,
    ROUND(SUM(event_event_type_count) / NULLIF(SUM(event_at_quarter_hour), 0), 3) AS event_event_type_count_to_event_at_quarter_hour_
FROM tagged_2
GROUP BY GROUPING SETS ((event_type, ticket_id), (event_type), (ticket_id))
ORDER BY event_type NULLS LAST, ticket_id NULLS LAST
