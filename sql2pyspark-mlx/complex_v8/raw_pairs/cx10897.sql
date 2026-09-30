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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 3 DAYS)
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at
    FROM in_window s
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        ticket_id,
        event_at,
        priority,
        agent_id,
        hours_since_open,
        first_event_at,
        DATE_TRUNC('DAY', last_event_at) AS last_event_at_day,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(first_event_at)) / 3600, 1) AS hours_first_event_at_to_event_at,
        ABS(hours_since_open - 50.4) AS hours_since_open_distance
    FROM with_stats
    WHERE ticket_id IS NOT NULL
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT ticket_id, priority, hours_first_event_at_to_event_at
    FROM labeled
) src
PIVOT (
    SUM(hours_first_event_at_to_event_at)
    FOR priority IN ('P4', 'P2', 'P1')
)
ORDER BY ticket_id
