WITH valid_agents AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_name,
        team,
        tier,
        DATE_ADD(hired_on, 30) AS hired_on_plus_30d
    FROM support_agents
    WHERE team IS NOT NULL
      AND hired_on < DATE '2025-03-30'
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        team,
        tier,
        hired_on_plus_30d,
        QUARTER(hired_on_plus_30d) AS hired_on_plus_30d_quarter,
        DATE_ADD(hired_on_plus_30d, 7) AS hired_on_plus_30d_plus_7d
    FROM valid_agents
),
annotated AS (
    -- Derive helper columns
    SELECT *, LOWER(REGEXP_REPLACE(TRIM(team), ' ', '_')) AS team_slug
    FROM scored
),
tagged AS (
    -- Add calculated columns
    SELECT *, DAYOFWEEK(hired_on_plus_30d) AS hired_on_plus_30d_dayofweek
    FROM annotated
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(hired_on_plus_30d) OVER w_tier_hired_on_plus_30d_dayof AS next_hired_on_plus_30d,
        SUM(hired_on_plus_30d_quarter) OVER (PARTITION BY hired_on_plus_30d_dayofweek ORDER BY hired_on_plus_30d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_hired_on_plus_30d_quarter,
        DENSE_RANK() OVER w_tier_hired_on_plus_30d_dayof_2 AS hired_on_plus_30d_dayofweek_dense_rank
    FROM tagged
    WINDOW w_tier_hired_on_plus_30d_dayof AS (PARTITION BY tier, hired_on_plus_30d_dayofweek ORDER BY hired_on_plus_30d_dayofweek DESC, id), w_tier_hired_on_plus_30d_dayof_2 AS (PARTITION BY tier, hired_on_plus_30d_dayofweek ORDER BY hired_on_plus_30d_dayofweek DESC)
),
leaders AS (
    -- First row per group
    SELECT
        id,
        team,
        tier,
        hired_on_plus_30d,
        hired_on_plus_30d_quarter,
        hired_on_plus_30d_plus_7d,
        team_slug,
        hired_on_plus_30d_dayofweek,
        next_hired_on_plus_30d,
        running_hired_on_plus_30d_quarter
    FROM sequenced
    WHERE hired_on_plus_30d_dayofweek_dense_rank = 1
      AND team IN ('Onboarding', 'Tech')
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM leaders
    WHERE hired_on_plus_30d_dayofweek > (SELECT MIN(hired_on_plus_30d_dayofweek) FROM leaders)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, metric_value
FROM (
    SELECT id, hired_on_plus_30d_quarter, hired_on_plus_30d_dayofweek
    FROM screened
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR field IN (hired_on_plus_30d_quarter, hired_on_plus_30d_dayofweek)
) unp
