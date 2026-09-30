WITH scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        full_name,
        dept_id,
        manager_id,
        job_title,
        hire_date,
        is_remote,
        AVG(salary) OVER (PARTITION BY dept_id ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_salary,
        MAX(salary) OVER (PARTITION BY dept_id ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_salary,
        COUNT(salary) OVER (PARTITION BY dept_id ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_salary,
        FIRST_VALUE(level, TRUE) OVER (PARTITION BY job_title ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_level
    FROM employees
    WHERE email LIKE '%@mail.co.th'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        full_name,
        dept_id,
        hire_date,
        running_max_salary,
        CUME_DIST() OVER w_ordered AS running_max_salary_cume_dist,
        LAST_VALUE(running_count_salary, TRUE) OVER (ORDER BY running_count_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_running_count_salary,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', hire_date), dept_id ORDER BY running_count_salary DESC, id) AS row_seq,
        NTILE(4) OVER w_hist AS running_max_salary_bucket
    FROM scored
    WINDOW w_ordered AS (ORDER BY running_max_salary NULLS FIRST), w_hist AS (PARTITION BY dept_id ORDER BY running_max_salary, id)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        dept_id,
        running_max_salary,
        row_seq,
        ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY running_max_salary_bucket, id) AS seq_no,
        ROW_NUMBER() OVER (ORDER BY row_seq DESC, id) AS seq_no_2,
        ROW_NUMBER() OVER (ORDER BY running_max_salary DESC, id) AS row_seq_2,
        ROUND(running_max_salary / SUM(running_max_salary) OVER (PARTITION BY dept_id), 4) AS running_max_salary_share
    FROM enriched
),
metrics_employees AS (
    -- Running and moving aggregates
    SELECT
        id,
        dept_id,
        running_max_salary,
        seq_no_2,
        running_max_salary_share,
        AVG(row_seq_2) OVER (PARTITION BY dept_id ORDER BY running_max_salary_share, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS row_seq_2_avg_3,
        NTILE(10) OVER w_part AS seq_no_2_bucket,
        LAG(running_max_salary, 2) OVER (PARTITION BY dept_id ORDER BY running_max_salary_share, id) AS prev_running_max_salary
    FROM metrics
    WINDOW w_part AS (PARTITION BY dept_id ORDER BY seq_no_2, id)
),
scored_employees AS (
    -- Running and moving aggregates
    SELECT
        id,
        dept_id,
        running_max_salary,
        row_seq_2_avg_3,
        NTILE(5) OVER (PARTITION BY dept_id ORDER BY running_max_salary, id) AS running_max_salary_bucket,
        NTILE(4) OVER (PARTITION BY dept_id ORDER BY row_seq_2_avg_3 DESC, id) AS row_seq_2_avg_3_bucket,
        AVG(prev_running_max_salary) OVER (ORDER BY prev_running_max_salary, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS prev_running_max_salary_avg_3_prior
    FROM metrics_employees
)
SELECT
    s.id,
    s.dept_id,
    s.running_max_salary,
    s.row_seq_2_avg_3,
    s.running_max_salary_bucket,
    s.row_seq_2_avg_3_bucket,
    s.prev_running_max_salary_avg_3_prior,
    (SELECT MIN(x.salary) FROM employees x WHERE x.dept_id = s.dept_id) AS min_employee_salary
FROM scored_employees s
ORDER BY row_seq_2_avg_3 NULLS LAST
