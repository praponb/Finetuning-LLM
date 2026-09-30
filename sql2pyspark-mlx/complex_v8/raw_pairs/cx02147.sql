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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 1 DAYS)
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        ticket_id,
        event_type,
        priority,
        agent_id,
        hours_since_open,
        FLOOR(hours_since_open / 10) * 10 AS hours_since_open_bucket,
        DATE_TRUNC('YEAR', event_at) AS event_at_year,
        LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug
    FROM matched
)
-- Final projection
SELECT
    id,
    event_type,
    agent_id,
    hours_since_open,
    hours_since_open_bucket,
    event_at_year
FROM tagged
ORDER BY agent_id
