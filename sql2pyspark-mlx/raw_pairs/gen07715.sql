WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(salary, 2) AS salary_rounded,
        COALESCE(level, 'N/A') AS level_filled
    FROM employees
    WHERE hire_date > DATE '2023-02-15'
      AND job_title IS NOT NULL
),
joined AS (
    -- Attach departments attributes
    SELECT src.*, b.budget
    FROM filtered src
    INNER JOIN departments b
        ON src.dept_id = b.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        full_name,
        email,
        dept_id,
        salary,
        hire_date,
        salary_rounded,
        level_filled,
        AVG(budget) OVER (PARTITION BY manager_id ORDER BY budget ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS budget_avg_2,
        PERCENT_RANK() OVER (PARTITION BY level, dept_id ORDER BY salary_rounded ASC) AS salary_rounded_percent_rank,
        LEAD(budget, 1) OVER (PARTITION BY manager_id, dept_id ORDER BY salary ASC, id ASC) AS next_budget
    FROM joined
    WHERE hire_date <= '2023-04-01'
),
joined_employees AS (
    -- Attach departments attributes
    SELECT
        s.id,
        s.full_name,
        s.dept_id,
        s.salary,
        s.hire_date,
        s.next_budget,
        d.dept_name
    FROM metrics s
    LEFT JOIN departments d
        ON s.dept_id = d.id
),
filtered_employees AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        next_budget - salary AS next_budget_minus_salary,
        DATE_TRUNC('WEEK', hire_date) AS hire_date_period
    FROM joined_employees
    WHERE hire_date > DATE '2023-05-11'
      AND hire_date > '2024-04-01'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY dept_name ORDER BY next_budget DESC, id ASC) AS next_budget_bucket,
        NTILE(3) OVER (PARTITION BY dept_name ORDER BY next_budget_minus_salary DESC, id ASC) AS next_budget_minus_salary_bucket,
        MAX(salary) OVER w_ordered AS salary_max_7d,
        LAST_VALUE(dept_name) IGNORE NULLS OVER (PARTITION BY dept_id ORDER BY next_budget_minus_salary ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_dept_name,
        SUM(salary) OVER w_recent AS hire_date_month_sum_salary
    FROM filtered_employees
    WINDOW w_ordered AS (PARTITION BY dept_id ORDER BY CAST(hire_date_period AS BIGINT) ASC RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY MONTH(hire_date), dept_name)
)
SELECT
    s.id,
    s.dept_id,
    s.salary,
    s.next_budget,
    s.dept_name,
    s.next_budget_minus_salary,
    s.hire_date_period,
    s.next_budget_bucket,
    s.next_budget_minus_salary_bucket,
    (SELECT SUM(x.salary) FROM employees x WHERE x.dept_id = s.dept_id) AS sum_employee_salary
FROM scored s;
