WITH scoped_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(actor), 1, 4) AS actor_prefix,
        DATE_FORMAT(event_at, 'MMM yyyy') AS event_at_label,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug
    FROM ticket_events
    WHERE event_type NOT IN ('reopened', 'resolved')
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        actor_prefix,
        event_at_label,
        event_type_slug,
        TO_DATE(event_at) AS event_at_date
    FROM scoped_events
    WHERE actor_prefix IS NOT NULL
)
SELECT
    ticket_id,
    event_type,
    GROUPING(ticket_id) AS is_total_ticket_id,
    MIN(event_at) AS first_event_at,
    ROUND(100 * COUNT(CASE WHEN event_type = 'assigned' THEN 1 END) / COUNT(*), 1) AS pct_event_type_assigned
FROM enriched
GROUP BY ROLLUP(ticket_id, event_type)
ORDER BY ticket_id NULLS LAST, event_type NULLS LAST
