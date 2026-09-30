WITH covered AS (
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
labeled AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(hours_since_open / 10) * 10 AS hours_since_open_bucket,
        CONCAT_WS(': ', priority, event_type) AS priority_event_type_label,
        DATE_TRUNC('MONTH', event_at) AS event_at_month
    FROM covered
)
SELECT *
FROM (
    SELECT event_at_month, priority, hours_since_open_bucket
    FROM labeled
) src
PIVOT (
    MIN(hours_since_open_bucket)
    FOR priority IN ('P3', 'P2', 'P4', 'P1')
)
ORDER BY event_at_month
