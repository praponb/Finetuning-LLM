WITH valid AS (
    SELECT *, CAST(budget AS INT) AS budget_int
    FROM departments
    WHERE dept_name IS NOT NULL
),
with_last AS (
    SELECT s.*, x.hire_date
    FROM valid s
    JOIN LATERAL (
            SELECT c.hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest_match AS (
    SELECT s.*, x.job_title
    FROM with_last s
    JOIN LATERAL (
            SELECT c.job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics AS (
    SELECT
        *,
        AVG(budget_int) OVER (PARTITION BY job_title ORDER BY budget, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS budget_int_avg_5,
        SUM(budget) OVER (PARTITION BY dept_name, job_title) AS dept_name_sum_budget,
        SUM(budget) OVER (PARTITION BY job_title ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_budget,
        AVG(budget_int) OVER (PARTITION BY dept_name ORDER BY budget_int, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS budget_int_avg_5_prior,
        ROW_NUMBER() OVER (ORDER BY budget, id) AS event_seq
    FROM latest_match
    WHERE budget_int = 21
)
SELECT
    location,
    CASE WHEN budget >= 4343000 THEN 'PREMIUM' ELSE 'STANDARD' END AS budget_tier,
    dept_name,
    COUNT(*) AS row_count,
    COUNT_IF(dept_name = 'Legal') AS legal_events,
    ROUND(AVG(budget_int), 2) AS avg_budget_int,
    APPROX_PERCENTILE(budget_int, ARRAY(0.25, 0.5, 0.75)) AS budget_int_quartiles
FROM metrics
GROUP BY ROLLUP (location, CASE WHEN budget >= 4343000 THEN 'PREMIUM' ELSE 'STANDARD' END, dept_name)
HAVING COUNT_IF(dept_name = 'Legal') >= 1
ORDER BY budget_tier DESC NULLS FIRST;
