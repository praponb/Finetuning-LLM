WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN is_remote THEN 'Yes' ELSE 'No' END AS is_remote_label,
        COALESCE(job_title, 'NONE') AS job_title_filled
    FROM employees
    WHERE is_remote
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id, level ORDER BY salary DESC, id) AS latest_rank
    FROM filtered
    QUALIFY latest_rank = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT dept_id, id
    FROM ranked
    EXCEPT ALL
    SELECT dept_id, id
    FROM employees
    WHERE salary > 58000
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    INNER JOIN remaining_keys k ON s.dept_id = k.dept_id AND s.id = k.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY level ORDER BY salary DESC) AS salary_rank,
        ROUND(salary / SUM(salary) OVER (PARTITION BY DATE_TRUNC('YEAR', hire_date), manager_id), 4) AS salary_share
    FROM verified
    WHERE manager_id IN (32, 34)
)
SELECT
    s.full_name,
    s.email,
    s.dept_id,
    s.manager_id,
    s.salary,
    s.hire_date,
    s.is_remote,
    s.job_title_filled,
    s.latest_rank,
    (SELECT MAX(t2.salary) FROM employees t2 WHERE t2.dept_id = s.dept_id) AS max_employee_salary
FROM with_windows s
ORDER BY max_employee_salary DESC;
