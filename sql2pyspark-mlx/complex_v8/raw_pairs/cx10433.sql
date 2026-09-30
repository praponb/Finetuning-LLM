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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 2 DAYS)
),
joined AS (
    -- Join support_agents
    SELECT s.*, su.agent_name, su.team AS agent_team
    FROM covered s
    JOIN support_agents su ON su.id = s.agent_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        agent_id,
        hours_since_open,
        agent_name,
        agent_team,
        FIRST_VALUE(hours_since_open) IGNORE NULLS OVER (PARTITION BY event_type ORDER BY hours_since_open DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_hours_since_open,
        LAG(event_at) OVER (PARTITION BY event_type ORDER BY hours_since_open DESC, id) AS prev_event_at,
        NTILE(3) OVER w_event_type_by_hours_since_op_2 AS hours_since_open_tercile,
        SUM(hours_since_open) OVER (PARTITION BY ticket_id, event_type) AS ticket_id_event_type_sum_hours_since_open
    FROM joined
    WINDOW w_event_type_by_hours_since_op_2 AS (PARTITION BY event_type ORDER BY hours_since_open DESC, id)
),
rollup_base AS (
    -- Group-level totals
    SELECT
        ticket_id,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_ym,
        ROUND(100 * COUNT(CASE WHEN agent_team = 'VIP' THEN 1 END) / COUNT(*), 1) AS pct_agent_team_vip,
        ARRAY_SORT(COLLECT_SET(agent_team)) AS agent_team_list,
        ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list,
        PERCENTILE_APPROX(hours_since_open, 0.5) AS median_hours_since_open
    FROM with_windows
    WHERE agent_team <> 'Billing'
    GROUP BY 1, 2
    HAVING MAX(hours_since_open) > 18.0
)
SELECT
    ticket_id,
    event_at_ym,
    pct_agent_team_vip,
    median_hours_since_open,
    CASE WHEN pct_agent_team_vip >= 60.0 THEN 'A' WHEN pct_agent_team_vip >= 30.0 THEN 'B' ELSE 'C' END AS pct_agent_team_vip_tier
FROM rollup_base
ORDER BY event_at_ym DESC, pct_agent_team_vip_tier DESC NULLS FIRST, ticket_id
LIMIT 20
