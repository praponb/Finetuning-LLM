WITH cleaned AS (
    -- Filter support_agents
    SELECT
        id,
        agent_name,
        team,
        hired_on,
        CASE WHEN team IN ('VIP', 'Onboarding') THEN 'group_a' WHEN team = 'Tech' THEN 'group_b' ELSE 'other' END AS team_group
    FROM support_agents
    WHERE tier IN ('T1', 'T3')
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        team,
        hired_on,
        LOWER(REGEXP_REPLACE(TRIM(team_group), ' ', '_')) AS team_group_slug,
        CONCAT_WS('-', agent_name, team) AS agent_name_team_label
    FROM cleaned
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        SUBSTRING(TRIM(team_group_slug), 1, 2) AS team_group_slug_prefix,
        CONCAT(team, '#', CAST(id AS STRING)) AS team_ref,
        COALESCE(team, 'UNKNOWN') AS team_or_unknown
    FROM derived
    WHERE agent_name_team_label IS NOT NULL
)
-- Subtotals with ROLLUP
SELECT
    team,
    team_or_unknown,
    GROUPING(team) AS is_total_team,
    SUM(CASE WHEN team IN ('Onboarding', 'Billing', 'VIP') THEN 1 ELSE 0 END) AS n_team_in_onboarding_billing_v
FROM tagged
GROUP BY ROLLUP(team, team_or_unknown)
