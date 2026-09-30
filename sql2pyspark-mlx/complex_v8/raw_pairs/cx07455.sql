WITH joined_ranges AS (
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
    -- Add calculated columns
    SELECT
        id,
        ticket_id,
        event_at,
        priority,
        LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug,
        COALESCE(hours_since_open, 0.0) AS hours_since_open_filled,
        FLOOR(hours_since_open / 10) * 10 AS hours_since_open_bucket
    FROM joined_ranges
    WHERE hours_since_open > 18.0
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    priority,
    priority_slug,
    hours_since_open_filled,
    hours_since_open_bucket,
    COALESCE(hours_since_open_filled, 0.0) AS hours_since_open_filled_filled
FROM scored
WHERE hours_since_open_bucket > 14
ORDER BY id NULLS LAST, event_at DESC
LIMIT 20
