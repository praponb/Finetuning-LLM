WITH cleaned AS (
    -- Scope the support_agents rows
    SELECT
        id,
        agent_name,
        team,
        tier
    FROM support_agents
    WHERE YEAR(hired_on) = 2024
      AND tier IN ('T3', 'T1')
),
filtered AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.id
              AND HOUR(t.closed_at) BETWEEN 7 AND 10
              AND SIZE(t.tags) >= 2
        )
      AND s.team = 'Onboarding'
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        agent_name,
        team,
        tier,
        LOWER(TRIM(agent_name)) AS agent_name_clean,
        CASE WHEN team IN ('Tech', 'VIP') THEN 'group_a' WHEN team = 'Billing' THEN 'group_b' ELSE 'other' END AS team_group,
        CONCAT_WS(': ', agent_name, tier) AS agent_name_tier_label
    FROM filtered
    WHERE team IS NOT NULL
)
SELECT
    id,
    team,
    tier,
    agent_name_clean,
    team_group,
    LENGTH(TRIM(agent_name_tier_label)) AS agent_name_tier_label_len,
    CONCAT_WS('-', team, tier) AS team_tier_label
FROM enriched
ORDER BY team_group DESC, id
