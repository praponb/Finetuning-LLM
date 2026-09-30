WITH in_window AS (
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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 2 DAYS)
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        priority,
        agent_id,
        LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug,
        FLOOR(hours_since_open / 10) * 10 AS hours_since_open_bucket,
        COALESCE(hours_since_open, 0.0) AS hours_since_open_filled
    FROM in_window
),
annotated_2 AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(hours_since_open_bucket - 29) AS hours_since_open_bucket_distance
    FROM annotated
)
-- Final projection
SELECT
    id,
    ticket_id,
    priority,
    agent_id,
    priority_slug,
    hours_since_open_filled,
    hours_since_open_bucket_distance
FROM annotated_2
ORDER BY priority_slug, id
LIMIT 5
