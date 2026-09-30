WITH valid AS (
    -- Scope the support_agents rows
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        COALESCE(team, 'UNKNOWN') AS team_or_unknown,
        COALESCE(tier, 'UNKNOWN') AS tier_or_unknown,
        TRUNC(hired_on, 'MM') AS hired_on_month_start
    FROM support_agents
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT agent_id, MAX(closed_at) AS last_ticket_closed_at
    FROM tickets
    GROUP BY agent_id
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.last_ticket_closed_at
    FROM valid s
    LEFT JOIN ticket_stats t ON t.agent_id = s.id
)
-- Subtotals with ROLLUP
SELECT
    team_or_unknown,
    tier,
    tier_or_unknown,
    ARRAY_SORT(COLLECT_SET(tier)) AS tier_list
FROM profiled
GROUP BY ROLLUP(team_or_unknown, tier, tier_or_unknown)
