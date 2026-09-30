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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 3 DAYS)
),
with_parent_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.priority,
        s.agent_id,
        su.hired_on AS agent_hired_on,
        su.agent_name,
        su.team AS agent_team
    FROM joined_ranges s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.hired_on IS NOT NULL
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        priority,
        agent_name,
        agent_team,
        CONCAT_WS(' / ', agent_name, agent_team) AS agent_name_agent_team_label,
        MONTH(agent_hired_on) AS agent_hired_on_month
    FROM with_parent_agent
    WHERE agent_hired_on >= DATE '2024-05-02'
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(priority, 'ALL') AS priority,
    COALESCE(agent_team, 'ALL') AS agent_team,
    GROUPING(priority) AS is_total_priority,
    COUNT(DISTINCT priority) AS distinct_prioritys,
    SUM(agent_hired_on_month) AS total_agent_hired_on_month
FROM labeled
GROUP BY ROLLUP(priority, agent_team)
ORDER BY priority NULLS LAST, agent_team NULLS LAST
