WITH base AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        csat,
        DATEDIFF(DATE '2025-12-31', closed_at) AS days_since_closed_at,
        CASE WHEN channel IN ('email', 'phone') THEN 'group_a' WHEN channel = 'web' THEN 'group_b' ELSE 'other' END AS channel_group,
        INITCAP(TRIM(priority)) AS priority_clean
    FROM tickets
    WHERE SIZE(tags) >= 2
      AND agent_id IS NOT NULL
),
screened AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.closed_at < TIMESTAMP '2025-02-21 07:00:00'
              AND t.channel IS NOT NULL
        )
      AND s.days_since_closed_at < 560
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.priority,
        s.days_since_closed_at,
        s.channel_group,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM screened s
    JOIN support_agents su ON su.id = s.agent_id AND su.team IN ('Onboarding', 'Tech')
)
SELECT
    priority,
    channel_group,
    GROUPING_ID() AS grouping_level,
    COUNT(DISTINCT priority) AS distinct_prioritys
FROM with_parent
GROUP BY GROUPING SETS ((priority, channel_group), (priority), (channel_group), ())
ORDER BY priority NULLS LAST, channel_group NULLS LAST
