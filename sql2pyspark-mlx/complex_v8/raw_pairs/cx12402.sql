WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug
    FROM ticket_events
    WHERE MONTH(event_at) IN (1, 2)
      AND ticket_id IS NOT NULL
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug,
        DATE_TRUNC('HOUR', event_at) AS event_at_hour
    FROM scoped
),
exploded_detail AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.actor_slug,
        s.event_at_hour,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.event_type AS event_event_type,
        t.actor AS event_actor
    FROM derived s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
)
SELECT
    event_event_type,
    event_type,
    COUNT(*) AS cnt,
    ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list,
    ROUND(100 * COUNT(CASE WHEN event_actor = 'system' THEN 1 END) / COUNT(*), 1) AS pct_event_actor_system
FROM exploded_detail
GROUP BY GROUPING SETS ((event_event_type, event_type), (event_event_type), (event_type))
