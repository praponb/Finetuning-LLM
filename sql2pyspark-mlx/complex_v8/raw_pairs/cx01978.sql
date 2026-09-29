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
totals AS (
    -- Group-level totals
    SELECT
        event_type,
        agent_id,
        priority,
        ROUND(AVG(hours_since_open), 2) AS avg_hours_since_open,
        MAX(event_at) AS last_event_at,
        ROUND(100 * COUNT(CASE WHEN event_type IN ('reopened', 'escalated') THEN 1 END) / COUNT(*), 1) AS pct_event_type_in_reopened_escal,
        MIN(hours_since_open) AS min_hours_since_open,
        ARRAY_SORT(COLLECT_SET(priority)) AS priority_list
    FROM covered
    GROUP BY event_type, agent_id, priority
    HAVING MAX(hours_since_open) > 7.2
),
with_details_agent AS (
    -- Attach agent attributes
    SELECT
        s.event_type,
        s.agent_id,
        s.priority,
        s.avg_hours_since_open,
        s.last_event_at,
        s.min_hours_since_open,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier
    FROM totals s
    JOIN support_agents su ON su.id = s.agent_id
)
SELECT
    event_type,
    agent_id,
    priority,
    last_event_at,
    min_hours_since_open,
    agent_team,
    agent_hired_on,
    agent_tier
FROM with_details_agent
ORDER BY agent_hired_on DESC NULLS FIRST, agent_team DESC NULLS FIRST
