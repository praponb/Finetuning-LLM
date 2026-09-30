WITH metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(salary, 1) OVER (PARTITION BY level ORDER BY hire_date, id) AS next_salary,
        AVG(salary) OVER (PARTITION BY DATE_TRUNC('week', hire_date), manager_id ORDER BY salary, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS salary_avg_5,
        AVG(salary) OVER (PARTITION BY dept_id, manager_id ORDER BY salary, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS salary_avg_3_prior,
        AVG(salary) OVER (PARTITION BY DATE_TRUNC('month', hire_date), dept_id ORDER BY hire_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS salary_avg_5_2,
        SUM(salary) OVER (PARTITION BY level, dept_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary
    FROM employees
),
metrics_employees AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(salary_avg_3_prior) OVER (ORDER BY salary, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS salary_avg_3_prior_avg_5_prior,
        SUM(salary_avg_5_2) OVER (PARTITION BY dept_id ORDER BY salary_avg_5, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_salary_avg_5_2,
        NTILE(10) OVER (PARTITION BY YEAR(hire_date) ORDER BY running_salary DESC, id) AS running_salary_bucket,
        SUM(salary_avg_3_prior) OVER (PARTITION BY MONTH(hire_date), dept_id ORDER BY running_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary_avg_3_prior
    FROM metrics
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(salary_avg_5 * 1.15, 2) AS adjusted_salary_avg_5,
        ROUND(running_salary_avg_3_prior / cumulative_salary_avg_5_2, 2) AS running_salary_avg_3_prior_per_cumulative_salary_avg_5_2
    FROM metrics_employees
    WHERE full_name LIKE 'Gra%'
),
joined AS (
    -- Attach employees attributes
    SELECT
        a.id,
        a.email,
        a.dept_id,
        a.manager_id,
        a.job_title,
        a.hire_date,
        a.is_remote,
        a.next_salary,
        a.salary_avg_5,
        a.salary_avg_5_2,
        a.running_salary,
        a.cumulative_salary_avg_5_2,
        a.running_salary_avg_3_prior,
        a.running_salary_avg_3_prior_per_cumulative_salary_avg_5_2,
        b.hire_date AS employee_hire_date,
        b.salary AS employee_salary
    FROM base a
    LEFT JOIN employees b
        ON a.manager_id = b.id
),
stats AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('WEEK', employee_hire_date) AS week_start,
        DATE_TRUNC('YEAR', hire_date) AS year_start,
        dept_id,
        SIZE(COLLECT_SET(job_title)) AS num_job_titles,
        COUNT_IF(job_title = 'Manager') AS manager_events,
        ROUND(SUM(cumulative_salary_avg_5_2) / NULLIF(COUNT(CASE WHEN job_title = 'Engineer' THEN 1 END), 0), 2) AS cumulative_salary_avg_5_2_per_engineer,
        SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list,
        SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list_2,
        RANK() OVER (PARTITION BY DATE_TRUNC('WEEK', employee_hire_date) ORDER BY SIZE(COLLECT_SET(job_title)) DESC) AS num_job_titles_rank
    FROM joined
    GROUP BY DATE_TRUNC('WEEK', employee_hire_date), DATE_TRUNC('YEAR', hire_date), dept_id
)
SELECT
    week_start,
    year_start,
    dept_id,
    manager_events,
    cumulative_salary_avg_5_2_per_engineer,
    num_job_titles_rank
FROM stats
