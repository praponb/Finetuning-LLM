WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        email,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        ROUND(salary * 1.1, 2) AS adjusted_salary
    FROM employees
    WHERE hire_date > DATE '2023-05-22'
),
enriched AS (
    -- Attach employees attributes
    SELECT
        src.*,
        b.full_name AS employee_full_name,
        b.level AS employee_level
    FROM valid src
    JOIN employees b
        ON src.manager_id = b.id
),
enriched_employees AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(adjusted_salary, 2) OVER (ORDER BY adjusted_salary, id) AS next_adjusted_salary,
        NTILE(5) OVER (ORDER BY adjusted_salary, id) AS adjusted_salary_bucket,
        NTILE(10) OVER (PARTITION BY job_title ORDER BY salary DESC, id) AS salary_bucket,
        ROW_NUMBER() OVER (PARTITION BY level, manager_id ORDER BY adjusted_salary, id) AS seq_no
    FROM enriched
    WHERE full_name LIKE '%FRANK%'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        manager_id,
        job_title,
        hire_date,
        is_remote,
        level,
        next_adjusted_salary,
        adjusted_salary_bucket,
        seq_no,
        FIRST_VALUE(job_title, TRUE) OVER w_ordered AS first_known_job_title,
        COUNT(adjusted_salary) OVER (PARTITION BY employee_level) AS employee_level_count_adjusted_salary,
        MIN(adjusted_salary) OVER (PARTITION BY job_title) AS job_title_min_adjusted_salary,
        AVG(salary_bucket) OVER (PARTITION BY MONTH(hire_date), employee_level ORDER BY salary, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS salary_bucket_avg_5
    FROM enriched_employees
    WINDOW w_ordered AS (PARTITION BY level, job_title ORDER BY salary_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    s.id,
    s.manager_id,
    s.job_title,
    s.hire_date,
    s.is_remote,
    s.level,
    s.next_adjusted_salary,
    s.first_known_job_title,
    s.employee_level_count_adjusted_salary,
    (SELECT MIN(x.salary) FROM employees x WHERE x.manager_id = s.manager_id) AS min_employee_salary
FROM metrics s
ORDER BY id
