WITH base AS (
    SELECT *, DATE_TRUNC('week', hire_date) AS hire_date_period
    FROM employees
    WHERE hire_date >= DATE '2024-01-15'
),
joined AS (
    SELECT
        a.*,
        p.dept_name,
        p.budget,
        p.location
    FROM base a
    LEFT JOIN departments p
        ON a.dept_id = p.id
),
with_windows AS (
    SELECT
        *,
        ROUND(budget / SUM(budget) OVER (PARTITION BY dept_id, level), 4) AS budget_share,
        ROUND(budget / SUM(budget) OVER (PARTITION BY dept_id, manager_id), 4) AS budget_share_2,
        AVG(salary) OVER (PARTITION BY job_title ORDER BY budget, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS salary_avg_6_prior
    FROM joined
),
totals AS (
    SELECT
        CASE WHEN budget >= 4193000 THEN 'PREMIUM' ELSE 'STANDARD' END AS budget_tier,
        PERCENTILE_APPROX(budget, 0.5) AS budget_p50,
        COUNT(DISTINCT location) AS distinct_locations,
        AVG(salary_avg_6_prior) AS avg_salary_avg_6_prior,
        ROUND(AVG(budget_share), 2) AS avg_budget_share,
        COUNT(*) AS num_rows
    FROM with_windows
    GROUP BY 1
)
SELECT
    budget_tier,
    avg_salary_avg_6_prior,
    num_rows
FROM totals
ORDER BY budget_tier NULLS FIRST, avg_salary_avg_6_prior DESC;
