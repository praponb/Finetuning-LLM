WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(salary) OVER (PARTITION BY dept_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_salary,
        MAX(salary) OVER (PARTITION BY dept_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_salary,
        SUM(salary) OVER (PARTITION BY dept_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_salary,
        AVG(salary) OVER (PARTITION BY MONTH(hire_date), level ORDER BY hire_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS salary_avg_5
    FROM employees
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY hire_date DESC, id) AS event_seq,
        SUM(running_max_salary) OVER (PARTITION BY job_title, manager_id ORDER BY salary_avg_5, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_max_salary
    FROM with_windows
),
with_dims AS (
    -- Attach departments attributes
    SELECT
        a.*,
        b.budget,
        b.dept_name
    FROM metrics a
    LEFT JOIN departments b
        ON a.dept_id = b.id
),
joined AS (
    -- Attach employees attributes
    SELECT
        a.id,
        a.manager_id,
        a.salary,
        a.hire_date,
        a.is_remote,
        a.level,
        a.running_count_salary,
        a.running_max_salary,
        a.salary_avg_5,
        a.event_seq,
        e.salary AS employee_salary,
        e.level AS employee_level,
        e.email AS employee_email
    FROM with_dims a
    LEFT JOIN employees e
        ON a.manager_id = e.id
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN running_max_salary >= 148000 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_max_salary_tier,
        CASE WHEN salary >= 145000 THEN 'HIGH' ELSE 'LOW' END AS salary_tier,
        ROUND(SUM(event_seq) / NULLIF(COUNT(CASE WHEN employee_level = 'L5' THEN 1 END), 0), 2) AS event_seq_per_l5,
        COUNT(DISTINCT employee_level) AS distinct_employee_levels,
        COUNT(*) FILTER (WHERE employee_level = 'L5') AS l5_rows
    FROM joined
    GROUP BY 1, 2
)
SELECT
    running_max_salary_tier,
    salary_tier,
    event_seq_per_l5,
    distinct_employee_levels,
    l5_rows
FROM totals
ORDER BY distinct_employee_levels NULLS FIRST;
