WITH prepared AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(event_type, '#', CAST(ticket_id AS STRING)) AS event_type_ref
    FROM ticket_events
    WHERE event_type IS NOT NULL
),
screened AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM prepared s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
        )
      AND TO_DATE(s.event_at) < DATE '2025-01-31'
),
grouped AS (
    -- Summarise per group
    SELECT
        actor,
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        COUNT(*) AS row_count,
        COUNT(DISTINCT event_type) AS distinct_event_types,
        COUNT(DISTINCT id) AS distinct_ids,
        ARRAY_SORT(COLLECT_SET(actor)) AS actor_list,
        COUNT(*) FILTER (WHERE event_type = 'created') AS n_event_type_created
    FROM screened
    WHERE event_type_ref IS NOT NULL
    GROUP BY actor, DATE_TRUNC('MONTH', event_at)
    HAVING COUNT(*) >= 3
)
SELECT
    actor,
    event_at_month,
    row_count,
    distinct_ids,
    actor_list,
    n_event_type_created
FROM grouped
ORDER BY n_event_type_created, actor, event_at_month
LIMIT 20
