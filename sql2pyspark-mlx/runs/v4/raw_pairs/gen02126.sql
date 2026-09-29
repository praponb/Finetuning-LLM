WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        is_remote,
        level,
        COALESCE(full_name, 'unassigned') AS full_name_filled
    FROM employees
    WHERE hire_date > DATE '2023-03-25'
      AND level IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        is_remote,
        level,
        full_name_filled,
        SUM(salary) OVER (ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary,
        ROW_NUMBER() OVER w_recent AS seq_no
    FROM filtered
    WINDOW w_recent AS (PARTITION BY level ORDER BY salary DESC, id)
),
enriched AS (
    -- Attach departments attributes
    SELECT s.*, b.dept_name
    FROM metrics s
    LEFT JOIN departments b
        ON s.dept_id = b.id
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
        is_remote,
        level,
        full_name_filled,
        running_salary,
        seq_no,
        dept_name,
        SUM(running_salary) OVER (PARTITION BY dept_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_salary,
        ROUND(salary / SUM(salary) OVER (PARTITION BY job_title), 4) AS salary_share,
        LAST_VALUE(job_title, TRUE) OVER (PARTITION BY dept_name ORDER BY running_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_job_title,
        AVG(salary) OVER (PARTITION BY level) AS level_avg_salary,
        ROUND(seq_no / SUM(seq_no) OVER (PARTITION BY job_title), 4) AS seq_no_share
    FROM enriched
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY manager_id, job_title ORDER BY seq_no_share DESC NULLS LAST, id) = 1
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        email,
        dept_id,
        job_title,
        salary,
        full_name_filled,
        running_salary,
        dept_name,
        cumulative_running_salary,
        salary_share,
        last_known_job_title,
        level_avg_salary,
        seq_no_share,
        CONCAT(dept_name, '/', email) AS dept_name_email,
        CASE WHEN is_remote THEN 'Yes' ELSE 'No' END AS is_remote_label
    FROM ranked
    WHERE level IS NOT NULL
      AND salary_share BETWEEN 0.48 AND 0.59
),
metrics_employees AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        full_name,
        email,
        dept_id,
        job_title,
        salary,
        full_name_filled,
        running_salary,
        dept_name,
        cumulative_running_salary,
        salary_share,
        last_known_job_title,
        level_avg_salary,
        seq_no_share,
        dept_name_email,
        is_remote_label,
        ROW_NUMBER() OVER (PARTITION BY dept_name, job_title ORDER BY seq_no_share, id) AS seq_no,
        ROUND(running_salary / SUM(running_salary) OVER (PARTITION BY dept_name), 4) AS running_salary_share,
        LEAD(salary_share, 1) OVER (PARTITION BY job_title ORDER BY salary, id) AS next_salary_share
    FROM valid
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        dept_id,
        salary_share,
        seq_no_share,
        next_salary_share
    FROM metrics_employees
) src
UNPIVOT (
    reading FOR measure IN (salary_share AS SALARY_SHARE_VAL, seq_no_share AS SEQ_NO_SHARE, next_salary_share AS NEXT_SALARY_SHARE)
)
