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
joined_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.priority,
        s.agent_id,
        s.hours_since_open,
        su.team AS agent_team,
        su.agent_name
    FROM joined_ranges s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT MAX(t.closed_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS last_ticket_closed_at,
        (SELECT MAX(t.closed_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS last_ticket_closed_at_2
    FROM joined_agent s
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        agent_team,
        ARRAY_SORT(COLLECT_SET(agent_name)) AS agent_name_list,
        COUNT(*) AS row_count,
        SUM(CASE WHEN priority IS NOT NULL THEN 1 ELSE 0 END) AS n_priority_is_not_null,
        ROUND(STDDEV(hours_since_open), 2) AS stddev_hours_since_open,
        MAX(hours_since_open) AS max_hours_since_open,
        COUNT(DISTINCT priority) AS distinct_prioritys
    FROM with_stats
    GROUP BY agent_team
)
SELECT
    agent_team,
    agent_name_list,
    row_count,
    stddev_hours_since_open,
    max_hours_since_open,
    distinct_prioritys,
    FLOOR(distinct_prioritys / 2) * 2 AS distinct_prioritys_bucket,
    CASE WHEN max_hours_since_open >= 50.4 THEN 'A' WHEN max_hours_since_open >= 28.8 THEN 'B' ELSE 'C' END AS max_hours_since_open_tier
FROM per_group
ORDER BY agent_team, row_count NULLS LAST
