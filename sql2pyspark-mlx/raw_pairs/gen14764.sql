WITH scored AS (
    -- Ranking and gap features
    SELECT
        id,
        budget,
        SUM(budget) OVER w_recent AS running_budget,
        ROUND(budget / SUM(budget) OVER (PARTITION BY location), 4) AS budget_share,
        AVG(budget) OVER w_grp AS budget_avg_5_prior,
        ROUND(budget / SUM(budget) OVER (PARTITION BY location), 4) AS budget_share_2
    FROM departments
    WINDOW w_recent AS (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.hire_date
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
base AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(budget_share, 2) AS budget_share_rounded
    FROM latest_match
    WHERE budget_avg_5_prior IS NOT NULL
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.dept_id IN (11, 24, 36, 39)
        )
),
eligible_departments AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND LOWER(c.full_name) LIKE '%avi%'
        )
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN budget >= 3225000 THEN 'PREMIUM' ELSE 'STANDARD' END AS budget_tier,
        DATE_TRUNC('QUARTER', hire_date) AS quarter_start,
        COUNT(DISTINCT id) AS distinct_ids,
        APPROX_PERCENTILE(budget_share, ARRAY(0.25, 0.5, 0.75)) AS budget_share_quartiles,
        APPROX_PERCENTILE(budget_share_2, 0.5) AS budget_share_2_p50
    FROM eligible_departments
    GROUP BY budget_tier, quarter_start
    HAVING APPROX_PERCENTILE(budget_share_2, 0.5) > 0
)
SELECT
    budget_tier,
    quarter_start,
    budget_share_quartiles,
    budget_share_2_p50
FROM totals;
