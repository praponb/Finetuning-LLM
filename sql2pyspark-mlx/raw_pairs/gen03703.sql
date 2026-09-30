WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        job_title,
        salary,
        is_remote,
        WEEKOFYEAR(hire_date) AS hire_date_weekofyear,
        COALESCE(level, 'UNKNOWN') AS resolved_level
    FROM employees
    WHERE hire_date > TO_DATE('2023-06-20')
),
cleaned_employees AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        job_title,
        salary,
        is_remote,
        resolved_level,
        CASE WHEN is_remote THEN 'Yes' ELSE 'No' END AS is_remote_label
    FROM cleaned
    WHERE job_title IS NOT DISTINCT FROM 'Intern'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        full_name,
        job_title,
        salary,
        is_remote,
        is_remote_label,
        SUM(salary) OVER w_recent AS cumulative_salary,
        SUM(salary) OVER w_recent AS cumulative_salary_2,
        ROUND(salary / SUM(salary) OVER (PARTITION BY job_title), 4) AS salary_share,
        SUM(salary) OVER w_recent AS cumulative_salary_3
    FROM cleaned_employees
    WHERE is_remote
    WINDOW w_recent AS (PARTITION BY job_title ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
kept_keys AS (
    -- Set intersection on keys
    SELECT job_title
    FROM enriched
    INTERSECT ALL
    SELECT job_title
    FROM employees
    WHERE manager_id IS NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.job_title = k.job_title
)
SELECT
    id,
    full_name,
    job_title,
    salary,
    is_remote,
    cumulative_salary,
    cumulative_salary_2,
    cumulative_salary_3
FROM verified
ORDER BY job_title DESC
