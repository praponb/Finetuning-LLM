WITH recent AS (
    -- Scope the support_agents rows
    SELECT
        id,
        team,
        tier,
        hired_on,
        CASE WHEN team IN ('VIP', 'Onboarding') THEN 'group_a' WHEN team = 'Billing' THEN 'group_b' ELSE 'other' END AS team_group
    FROM support_agents
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (PARTITION BY tier, team ORDER BY hired_on DESC, id) AS hired_on_rank,
        FIRST_VALUE(hired_on) OVER (PARTITION BY tier, team ORDER BY hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_hired_on,
        ROUND(CUME_DIST() OVER (PARTITION BY tier, team ORDER BY hired_on DESC, id), 3) AS hired_on_cume_dist,
        NTH_VALUE(hired_on, 2) OVER (PARTITION BY tier, team ORDER BY hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_hired_on,
        LEAD(hired_on, 3) OVER w_tier_team_by_hired_on AS next3_hired_on
    FROM recent
    WINDOW w_tier_team_by_hired_on AS (PARTITION BY tier, team ORDER BY hired_on DESC, id)
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        hired_on_cume_dist + hired_on_rank AS hired_on_cume_dist_plus_hired_on_rank,
        LENGTH(TRIM(team_group)) AS team_group_len,
        LOWER(REGEXP_REPLACE(TRIM(team), ' ', '_')) AS team_slug
    FROM sequenced
),
qualified AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM tagged s
    WHERE s.id IN (
            SELECT t.agent_id
            FROM tickets t
        )
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM qualified
    WHERE team_group_len > (SELECT AVG(team_group_len) FROM qualified)
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(hired_on_cume_dist * 1.5, 2) AS hired_on_cume_dist_adj,
        CONCAT_WS(': ', team, team_slug) AS team_team_slug_label
    FROM without_matches
)
SELECT
    id,
    tier,
    team_group,
    first_hired_on,
    hired_on_cume_dist,
    team_slug,
    team_team_slug_label,
    CASE WHEN hired_on_cume_dist_adj >= 1.0 THEN 'HIGH' WHEN hired_on_cume_dist_adj >= 0.6 THEN 'MEDIUM' ELSE 'LOW' END AS hired_on_cume_dist_adj_tier,
    CASE WHEN team IN ('Tech', 'Billing') THEN 'group_a' WHEN team = 'Onboarding' THEN 'group_b' ELSE 'other' END AS team_group_2
FROM annotated
WHERE YEAR(second_hired_on) = 2024
ORDER BY first_hired_on NULLS LAST, team_slug DESC
