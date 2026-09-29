WITH cleaned AS (
    SELECT
        *,
        LOWER(job_title) AS job_title_lower,
        CAST(is_remote AS INT) AS is_remote_flag
    FROM employees
    WHERE YEAR(hire_date) >= 2024
),
joined AS (
    SELECT
        src.*,
        b.dept_name,
        b.budget
    FROM cleaned src
    INNER JOIN departments b
        ON src.dept_id = b.id
    WHERE b.location IN ('New York', 'Singapore', 'Tokyo')
),
remaining_keys AS (
    SELECT job_title
    FROM joined
    INTERSECT
    SELECT job_title
    FROM employees
    WHERE salary > 103000
),
reconciled AS (
    SELECT s.*
    FROM joined s
    INNER JOIN remaining_keys k ON s.job_title = k.job_title
),
joined_employees AS (
    SELECT s.*, d.job_title AS employee_job_title
    FROM reconciled s
    INNER JOIN employees d
        ON s.manager_id = d.id
),
scored AS (
    SELECT *, SUM(is_remote_flag) OVER w_grp AS manager_id_sum_is_remote_flag
    FROM joined_employees
    WINDOW w_grp AS (PARTITION BY manager_id)
)
SELECT
    d.location,
    src.job_title,
    COUNT(*) AS rows_total,
    MIN(src.is_remote_flag) AS min_is_remote_flag,
    SUM(src.budget) AS total_budget,
    GROUPING_ID() AS level_id
FROM scored src
INNER JOIN departments d ON src.dept_id = d.id
GROUP BY ROLLUP (d.location, src.job_title);
