WITH base_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        tags,
        CASE WHEN priority IN ('P4', 'P1') THEN 'group_a' WHEN priority = 'P3' THEN 'group_b' ELSE 'other' END AS priority_group,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 3600, 1) AS hours_opened_at_to_closed_at
    FROM tickets
    WHERE channel = 'email'
      AND csat > 2
),
qualified AS (
    -- Keep rows whose agent matches
    SELECT s.*
    FROM base_tickets s
    WHERE NOT EXISTS (
            SELECT 1
            FROM support_agents su
            WHERE su.id = s.agent_id
              AND su.team = 'Onboarding'
              AND su.hired_on BETWEEN DATE '2024-05-02' AND DATE '2025-11-09'
        )
),
screened AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM qualified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.event_type IS NOT NULL
              AND t.actor IS NOT NULL
        )
      AND SIZE(s.tags) >= 1
),
per_label AS (
    -- One row per tag
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.priority_group,
        s.hours_opened_at_to_closed_at,
        t.tag
    FROM screened s
    LATERAL VIEW OUTER EXPLODE(s.tags) t AS tag
),
enriched AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.priority_group,
        s.hours_opened_at_to_closed_at,
        s.tag,
        su.tier AS agent_tier,
        su.agent_name,
        su.team AS agent_team
    FROM per_label s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.team NOT IN ('Billing', 'Tech')
)
SELECT
    id,
    requester_id,
    priority_group,
    hours_opened_at_to_closed_at,
    tag,
    agent_name,
    agent_team,
    COALESCE(agent_team, 'UNKNOWN') AS agent_team_or_unknown,
    CASE WHEN agent_team IN ('VIP', 'Onboarding') THEN 'group_a' WHEN agent_team = 'Tech' THEN 'group_b' ELSE 'other' END AS agent_team_group
FROM enriched
ORDER BY agent_name, tag NULLS LAST
