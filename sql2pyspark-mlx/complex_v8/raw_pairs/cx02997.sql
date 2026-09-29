WITH filtered_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
        UPPER(TRIM(actor)) AS actor_clean,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE event_type IS NOT NULL
),
joined AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.actor_slug,
        t.priority AS ticket_priority
    FROM filtered_events s
    JOIN tickets t ON t.id = s.ticket_id
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        ticket_id,
        event_type,
        actor_slug,
        ticket_priority,
        LOWER(REGEXP_REPLACE(TRIM(ticket_priority), ' ', '_')) AS ticket_priority_slug,
        SUBSTRING(TRIM(event_type), 1, 3) AS event_type_prefix,
        COALESCE(ticket_priority, 'UNKNOWN') AS ticket_priority_or_unknown
    FROM joined
)
SELECT
    ticket_id,
    COALESCE(event_type, 'ALL') AS event_type,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT id) AS distinct_ids
FROM enriched
GROUP BY CUBE(ticket_id, event_type)
ORDER BY ticket_id NULLS LAST, event_type NULLS LAST
