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
rollup_base AS (
    -- Summarise per group
    SELECT
        priority,
        agent_id,
        event_type,
        ROUND(AVG(hours_since_open), 2) AS avg_hours_since_open,
        ROUND(STDDEV(hours_since_open), 2) AS stddev_hours_since_open,
        COUNT(*) FILTER (WHERE priority = 'P4') AS n_priority_p4,
        MIN(event_at) AS first_event_at,
        COUNT(*) AS cnt
    FROM covered
    WHERE hours_since_open > 28.8
    GROUP BY priority, agent_id, event_type
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT n_priority_p4, event_type, cnt
    FROM rollup_base
) src
PIVOT (
    MIN(cnt) AS min_v, COUNT(cnt) AS n
    FOR event_type IN ('escalated', 'assigned', 'replied')
)
ORDER BY n_priority_p4
