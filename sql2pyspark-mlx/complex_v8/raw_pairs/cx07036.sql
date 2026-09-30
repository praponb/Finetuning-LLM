WITH filtered AS (
    -- Scope the support_agents rows
    SELECT
        id,
        agent_name,
        team,
        hired_on,
        DATE_TRUNC('YEAR', hired_on) AS hired_on_year,
        LOWER(TRIM(team)) AS team_clean
    FROM support_agents
    WHERE tier = 'T3'
),
labeled AS (
    -- Label and score rows
    SELECT *, CONCAT(agent_name, '#', CAST(id AS STRING)) AS agent_name_ref
    FROM filtered
),
summary AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', hired_on) AS hired_on_month,
        DATE_FORMAT(hired_on_year, 'yyyy-MM') AS hired_on_year_ym,
        team,
        SUM(CASE WHEN team IN ('Tech', 'Billing', 'Onboarding') THEN 1 ELSE 0 END) AS n_team_in_tech_billing_onboard,
        COUNT(DISTINCT team) AS distinct_teams
    FROM labeled
    GROUP BY 1, 2, 3
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_distinct_teams_hired_on_mont, 3) AS distinct_teams_percent_rank,
        NTH_VALUE(hired_on_month, 2) OVER (PARTITION BY distinct_teams, TO_DATE(hired_on_month) ORDER BY distinct_teams ASC, hired_on_month, hired_on_year_ym, team ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_hired_on_month
    FROM summary
    WINDOW w_distinct_teams_hired_on_mont AS (PARTITION BY distinct_teams, TO_DATE(hired_on_month) ORDER BY distinct_teams ASC, hired_on_month, hired_on_year_ym, team)
),
filtered_2 AS (
    -- Compare against the overall avg
    SELECT *
    FROM windowed
    WHERE n_team_in_tech_billing_onboard > (SELECT AVG(n_team_in_tech_billing_onboard) FROM windowed)
),
with_activity_2 AS (
    -- Compare against the overall max
    SELECT *
    FROM filtered_2
    WHERE distinct_teams < (SELECT MAX(distinct_teams) FROM filtered_2)
)
-- Final projection
SELECT
    hired_on_month,
    hired_on_year_ym,
    team,
    n_team_in_tech_billing_onboard,
    second_hired_on_month,
    CASE WHEN team IN ('Billing', 'Onboarding') THEN 'group_a' WHEN team = 'VIP' THEN 'group_b' ELSE 'other' END AS team_group,
    LOWER(TRIM(hired_on_year_ym)) AS hired_on_year_ym_clean
FROM with_activity_2
WHERE team = 'Billing'
ORDER BY team
