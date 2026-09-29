WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, TRIM(level) AS clean_level
    FROM employees
    WHERE hire_date > DATE '2023-03-04'
      AND level IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(salary) OVER w_hist AS salary_avg_2,
        LAST_VALUE(job_title, TRUE) OVER w_ordered AS last_known_job_title,
        SUM(salary) OVER w_grp AS cumulative_salary,
        MAX(salary) OVER (PARTITION BY manager_id) AS manager_id_max_salary
    FROM valid
    WINDOW w_hist AS (PARTITION BY dept_id ORDER BY hire_date ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY level ORDER BY salary ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY job_title ORDER BY hire_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
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
        clean_level,
        salary_avg_2,
        last_known_job_title,
        cumulative_salary,
        manager_id_max_salary,
        NTILE(5) OVER (PARTITION BY manager_id, job_title ORDER BY manager_id_max_salary ASC, id ASC) AS manager_id_max_salary_bucket,
        SUM(salary) OVER (PARTITION BY manager_id, job_title ORDER BY cumulative_salary ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary,
        ROW_NUMBER() OVER (ORDER BY hire_date DESC, id ASC) AS event_seq,
        AVG(cumulative_salary) OVER (PARTITION BY level ORDER BY hire_date ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS cumulative_salary_avg_6_prior
    FROM enriched
),
with_dims AS (
    -- Attach departments attributes
    SELECT
        src.*,
        d.budget,
        d.location,
        d.dept_name
    FROM with_windows src
    INNER JOIN departments d
        ON src.dept_id = d.id
),
valid_employees AS (
    -- Filter the raw rows and derive helper columns
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
        clean_level,
        last_known_job_title,
        cumulative_salary,
        manager_id_max_salary,
        running_salary,
        cumulative_salary_avg_6_prior,
        budget,
        location,
        dept_name,
        LENGTH(location) AS location_length,
        COALESCE(event_seq, 0) AS event_seq_filled
    FROM with_dims
    WHERE hire_date >= TO_DATE('2024-03-01')
      AND location IS NOT NULL
)
SELECT
    s.id,
    s.full_name,
    s.email,
    s.dept_id,
    s.hire_date,
    s.is_remote,
    s.clean_level,
    s.last_known_job_title,
    s.cumulative_salary,
    (SELECT SUM(t2.salary) FROM employees t2 WHERE t2.dept_id = s.dept_id) AS sum_employee_salary
FROM valid_employees s
