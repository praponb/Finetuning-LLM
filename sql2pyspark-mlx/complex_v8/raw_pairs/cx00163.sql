WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND actor IS NOT NULL
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        event_type,
        COUNT(*) AS cnt,
        MIN(event_at) AS first_event_at,
        COUNT(DISTINCT ticket_id) AS distinct_ticket_ids
    FROM filtered
    GROUP BY event_type
),
enriched AS (
    -- Add calculated columns
    SELECT
        event_type,
        cnt,
        LENGTH(TRIM(event_type)) AS event_type_len,
        cnt - distinct_ticket_ids AS cnt_minus_distinct_ticket_ids,
        TO_DATE(first_event_at) AS first_event_at_date
    FROM stats
)
SELECT event_type, measure, metric_value
FROM (
    SELECT event_type, cnt, event_type_len
    FROM enriched
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR measure IN (cnt, event_type_len)
) unp
ORDER BY event_type, measure
