WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        IF(is_remote, 'Yes', 'No') AS is_remote_label,
        WEEKOFYEAR(hire_date) AS hire_date_weekofyear
    FROM employees
    WHERE level IS DISTINCT FROM 'L3'
      AND hire_date >= DATE '2023-08-24'
),
latest AS (
    -- Keep one row per key (latest first)
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
        is_remote_label,
        hire_date_weekofyear
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY hire_date ASC NULLS FIRST, id ASC) = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        dept_id,
        job_title,
        salary,
        is_remote,
        LAG(salary) OVER w_running AS prev_salary,
        LAG(hire_date_weekofyear) OVER w_running AS prev_hire_date_weekofyear,
        ROW_NUMBER() OVER w_running AS seq_no,
        FIRST_VALUE(job_title) IGNORE NULLS OVER (PARTITION BY job_title ORDER BY hire_date_weekofyear ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_job_title
    FROM latest
    WHERE full_name LIKE 'O%'
    WINDOW w_running AS (PARTITION BY level ORDER BY salary ASC, id ASC)
),
with_dims AS (
    -- Attach departments attributes
    SELECT s.*, p.location
    FROM with_windows s
    LEFT JOIN departments p
        ON s.dept_id = p.id
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN salary >= 140000 THEN 'LARGE' ELSE 'SMALL' END AS salary_tier,
    MAX(salary) AS max_salary,
    MAX(salary) FILTER (WHERE location = 'New York') AS new_york_max_salary,
    SUM(CASE WHEN job_title = 'Director' THEN prev_hire_date_weekofyear ELSE 0 END) AS director_prev_hire_date_weekofyear
FROM with_dims
GROUP BY CASE WHEN salary >= 140000 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY new_york_max_salary DESC NULLS FIRST;
