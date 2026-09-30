WITH base AS (
    -- Keep recent, valid rows
    SELECT *, INITCAP(TRIM(full_name)) AS full_name_title
    FROM employees
    WHERE hire_date > DATE '2023-05-06'
      AND job_title IS NOT NULL
),
with_dims AS (
    -- Attach employees attributes
    SELECT
        s.*,
        b.email AS employee_email,
        b.job_title AS employee_job_title,
        b.salary AS employee_salary
    FROM base s
    LEFT JOIN employees b
        ON s.manager_id = b.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        full_name_title,
        employee_email,
        employee_job_title,
        employee_salary,
        AVG(salary) OVER (PARTITION BY dept_id ORDER BY salary, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS salary_avg_2,
        AVG(salary) OVER (PARTITION BY manager_id ORDER BY salary, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS salary_avg_3_prior
    FROM with_dims
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        full_name_title,
        employee_email,
        employee_job_title,
        employee_salary,
        salary_avg_2,
        salary_avg_3_prior,
        NTILE(3) OVER w_running AS salary_avg_2_bucket,
        AVG(salary_avg_2) OVER w_hist AS salary_avg_2_avg_6_prior,
        SUM(salary) OVER w_hist_2 AS running_salary,
        ROUND(salary_avg_2 / SUM(salary_avg_2) OVER (PARTITION BY job_title), 4) AS salary_avg_2_share,
        NTILE(10) OVER (ORDER BY salary_avg_3_prior, id) AS salary_avg_3_prior_bucket
    FROM with_windows
    WINDOW w_running AS (PARTITION BY employee_job_title ORDER BY salary_avg_2, id), w_hist AS (PARTITION BY manager_id ORDER BY salary, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), w_hist_2 AS (PARTITION BY job_title ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY employee_job_title ORDER BY salary_avg_3_prior_bucket NULLS LAST, id) AS rank_in_group
    FROM scored
    QUALIFY rank_in_group = 1
),
ranked_employees AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_job_title ORDER BY salary_avg_2_bucket DESC NULLS FIRST, id) = 1
)
SELECT
    s.id,
    s.full_name,
    s.email,
    s.manager_id,
    s.job_title,
    s.salary,
    s.hire_date,
    s.is_remote,
    s.level,
    (SELECT MAX(t2.salary) FROM employees t2 WHERE t2.manager_id = s.manager_id) AS max_employee_salary
FROM ranked_employees s
ORDER BY job_title;
