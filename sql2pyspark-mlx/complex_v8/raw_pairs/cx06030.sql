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
tagged AS (
    -- Derive helper columns
    SELECT *, COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
    FROM in_window
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        event_at,
        event_type,
        hours_since_open,
        event_type_or_unknown,
        hours_since_open - LAG(hours_since_open) OVER (PARTITION BY agent_id, event_type_or_unknown ORDER BY event_at, id) AS hours_since_open_change,
        FIRST_VALUE(hours_since_open) OVER (PARTITION BY TO_DATE(event_at) ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_hours_since_open
    FROM tagged
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM with_windows
    WHERE first_hours_since_open < (SELECT MAX(first_hours_since_open) FROM with_windows)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT event_type, event_type_or_unknown, hours_since_open
    FROM filtered
) src
PIVOT (
    AVG(hours_since_open)
    FOR event_type_or_unknown IN ('assigned', 'UNKNOWN')
)
