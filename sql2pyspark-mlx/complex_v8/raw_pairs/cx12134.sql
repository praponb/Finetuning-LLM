WITH matched AS (
    -- Events that happened while the ticket was open
    SELECT
        e.id,
        e.ticket_id,
        e.event_at,
        e.event_type,
        t.priority,
        t.agent_id,
        ROUND((UNIX_TIMESTAMP(e.event_at) - UNIX_TIMESTAMP(t.opened_at)) / 3600, 2) AS hours_since_open
    FROM ticket_events e
    JOIN tickets t
      ON t.id = e.ticket_id
     AND e.event_at >= t.opened_at
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 7 DAYS)
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        event_type,
        priority,
        agent_id,
        hours_since_open,
        FLOOR(hours_since_open / 10) * 10 AS hours_since_open_bucket,
        CONCAT(event_type, '#', CAST(id AS STRING)) AS event_type_ref
    FROM matched
    WHERE hours_since_open BETWEEN 18.0 AND 68.4
),
summary AS (
    -- Summarise per group
    SELECT
        priority,
        ROUND(SUM(hours_since_open), 2) AS total_hours_since_open,
        ROUND(SUM(hours_since_open_bucket) / NULLIF(SUM(hours_since_open), 0), 3) AS hours_since_open_bucket_to_hours_since_open_rati,
        MIN(hours_since_open_bucket) AS min_hours_since_open_bucket,
        SUM(hours_since_open_bucket) AS total_hours_since_open_bucket,
        ARRAY_SORT(COLLECT_SET(priority)) AS priority_list,
        MAX(hours_since_open) AS max_hours_since_open
    FROM scored
    GROUP BY priority
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        max_hours_since_open + total_hours_since_open AS max_hours_since_open_plus_total_hours_since_open,
        GREATEST(total_hours_since_open, max_hours_since_open) AS greatest_total_hours_since_open_max_hours_since_,
        ABS(max_hours_since_open - 25.2) AS max_hours_since_open_distance
    FROM summary
)
-- Final projection
SELECT
    priority,
    total_hours_since_open,
    min_hours_since_open_bucket,
    total_hours_since_open_bucket,
    priority_list,
    max_hours_since_open,
    max_hours_since_open_plus_total_hours_since_open,
    greatest_total_hours_since_open_max_hours_since_,
    max_hours_since_open_distance,
    LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug
FROM annotated
WHERE greatest_total_hours_since_open_max_hours_since_ < 280.8
ORDER BY max_hours_since_open_distance DESC NULLS FIRST, priority
LIMIT 10
