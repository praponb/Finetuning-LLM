WITH joined AS (
    -- Attach departments attributes
    SELECT
        src.*,
        p.budget,
        p.dept_name,
        p.location
    FROM employees src
    LEFT JOIN departments p
        ON src.dept_id = p.id
    WHERE p.location IN ('Chiang Mai', 'Bangkok', 'Berlin')
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(salary) OVER (PARTITION BY level, manager_id ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_salary,
        MIN(salary) OVER (PARTITION BY level, manager_id ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_salary,
        MAX(salary) OVER (PARTITION BY level, manager_id ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_salary,
        PERCENT_RANK() OVER (PARTITION BY location, dept_name ORDER BY salary) AS salary_percent_rank
    FROM joined
),
with_dims AS (
    -- Attach departments attributes
    SELECT src.*, p.location AS department_location
    FROM enriched src
    LEFT JOIN departments p
        ON src.dept_id = p.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(salary) OVER w_running AS last4_max_salary,
        SUM(salary) OVER w_running AS last4_sum_salary,
        MIN(salary) OVER w_running AS last4_min_salary,
        AVG(salary) OVER w_running AS last4_avg_salary,
        MAX(salary) OVER w_grp AS hire_date_period_max_salary
    FROM with_dims
    WINDOW w_running AS (PARTITION BY department_location ORDER BY budget, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY DATE_TRUNC('week', hire_date))
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    dept_name,
    metric,
    value
FROM (
    SELECT
        id,
        dept_name,
        salary,
        budget
    FROM with_windows
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric IN (salary AS SALARY, budget AS BUDGET)
);
