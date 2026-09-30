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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 3 DAYS)
),
summary AS (
    -- Group-level totals
    SELECT
        ticket_id,
        ROUND(STDDEV(hours_since_open), 2) AS stddev_hours_since_open,
        MAX(hours_since_open) AS max_hours_since_open,
        COUNT(*) AS cnt
    FROM covered
    WHERE MONTH(event_at) IN (2, 3)
    GROUP BY ticket_id
    HAVING COUNT(*) >= 2
),
scored AS (
    -- Derive helper columns
    SELECT
        ticket_id,
        stddev_hours_since_open,
        max_hours_since_open,
        ROUND(stddev_hours_since_open * 1.1, 2) AS stddev_hours_since_open_adj
    FROM summary
    WHERE stddev_hours_since_open < 64.8
)
-- Native SQL UNPIVOT (wide to long)
SELECT ticket_id, metric_name, amount
FROM (
    SELECT ticket_id, max_hours_since_open, stddev_hours_since_open
    FROM scored
) src
UNPIVOT (
    amount FOR metric_name IN (max_hours_since_open AS max_hours_since_open_v, stddev_hours_since_open AS stddev_hours_since_open_v)
) unp
ORDER BY ticket_id, metric_name
