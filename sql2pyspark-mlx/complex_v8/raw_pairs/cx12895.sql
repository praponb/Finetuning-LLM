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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 7 DAYS)
),
with_parent_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.agent_id,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM in_window s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
screened AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM with_parent_agent s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_type <> 'reopened'
              AND t.event_at >= TIMESTAMP '2025-01-25 11:00:00'
        )
),
latest_per_key AS (
    -- Keep the latest row per agent_team, event_type
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        agent_id,
        agent_tier,
        agent_team
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_team, event_type ORDER BY event_at DESC, id DESC) AS row_num
        FROM screened
    ) ranked
    WHERE row_num = 1
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    agent_id,
    agent_tier,
    agent_team,
    CASE WHEN agent_team IN ('Billing', 'Onboarding') THEN 'group_a' WHEN agent_team = 'VIP' THEN 'group_b' ELSE 'other' END AS agent_team_group
FROM latest_per_key
ORDER BY agent_id NULLS LAST
