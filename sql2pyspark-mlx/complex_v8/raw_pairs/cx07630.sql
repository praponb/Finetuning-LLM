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
    -- Summarise per group
    SELECT
        agent_id,
        priority,
        ARRAY_SORT(COLLECT_SET(priority)) AS priority_list,
        ROUND(STDDEV(hours_since_open), 2) AS stddev_hours_since_open,
        SUM(hours_since_open) AS total_hours_since_open,
        COUNT(*) AS row_count,
        COUNT(DISTINCT priority) AS distinct_prioritys
    FROM covered
    GROUP BY 1, 2
    HAVING COUNT(*) >= 2
),
enriched_agent AS (
    -- Attach agent attributes
    SELECT s.*, su.hired_on AS agent_hired_on, su.team AS agent_team
    FROM summary s
    LEFT JOIN support_agents su ON su.id = s.agent_id
)
-- Final projection
SELECT
    agent_id,
    priority,
    priority_list,
    stddev_hours_since_open,
    total_hours_since_open,
    distinct_prioritys,
    agent_hired_on,
    DATE_TRUNC('YEAR', agent_hired_on) AS agent_hired_on_year
FROM enriched_agent
ORDER BY agent_hired_on_year, stddev_hours_since_open
