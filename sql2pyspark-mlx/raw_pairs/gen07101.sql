WITH valid AS (
    SELECT *, CONCAT_WS('-', job_title, email) AS job_title_email_key
    FROM employees
    WHERE hire_date > DATE '2023-09-08'
),
scored AS (
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        level,
        AVG(salary) OVER (PARTITION BY manager_id ORDER BY hire_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS salary_avg_6_prior,
        ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary, id) AS row_seq,
        ROUND(salary / SUM(salary) OVER (PARTITION BY manager_id), 4) AS salary_share
    FROM valid
),
with_windows AS (
    SELECT
        *,
        SUM(row_seq) OVER (PARTITION BY manager_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_row_seq,
        MIN(row_seq) OVER (PARTITION BY manager_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_row_seq,
        AVG(row_seq) OVER (PARTITION BY manager_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_row_seq
    FROM scored
    WHERE salary BETWEEN 112000 AND 139000
),
kept_keys AS (
    SELECT dept_id, level
    FROM with_windows
    INTERSECT
    SELECT dept_id, level
    FROM employees
    WHERE dept_id IN (25, 28, 40)
),
verified AS (
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys k ON s.dept_id = k.dept_id AND s.level = k.level
),
base AS (
    SELECT
        full_name,
        email,
        dept_id,
        job_title,
        level,
        salary_share,
        running_min_row_seq,
        running_avg_row_seq,
        COALESCE(running_sum_row_seq, 0) AS running_sum_row_seq_filled,
        REPLACE(full_name, ' ', '') AS full_name_slug
    FROM verified
    WHERE row_seq = 3
)
SELECT
    p.location,
    e.job_title,
    COUNT(*) AS cnt,
    MIN(e.salary_share) AS min_salary_share,
    GROUPING_ID() AS level_id
FROM base e
INNER JOIN departments p ON e.dept_id = p.id
GROUP BY GROUPING SETS ((p.location, e.job_title), (p.location), ())
ORDER BY cnt NULLS FIRST;
