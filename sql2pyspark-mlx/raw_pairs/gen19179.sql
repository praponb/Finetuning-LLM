WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        email,
        dept_id,
        manager_id,
        salary,
        hire_date,
        level,
        COALESCE(job_title, 'Unknown') AS job_title_filled
    FROM employees
    WHERE email LIKE '%@yahoo.com'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        email,
        dept_id,
        salary,
        job_title_filled,
        COALESCE(email, 'N/A') AS email_filled
    FROM cleaned
    WHERE level IS NOT NULL
),
valid_employees AS (
    -- Basic cleansing and defaults
    SELECT *, LENGTH(email) AS email_length
    FROM valid
    WHERE dept_id IN (2, 14, 24)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        dept_id,
        salary,
        job_title_filled,
        MAX(salary) OVER (PARTITION BY dept_id) AS dept_id_max_salary,
        CUME_DIST() OVER (PARTITION BY dept_id ORDER BY email_length NULLS LAST) AS email_length_cume_dist,
        LAST_VALUE(email_length, TRUE) OVER (PARTITION BY dept_id ORDER BY email_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_email_length,
        LAG(email_length, 2) OVER (PARTITION BY dept_id ORDER BY email_length, id) AS prev_email_length,
        SUM(salary) OVER (ORDER BY email_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_salary
    FROM valid_employees
),
remaining_keys AS (
    -- Set difference on keys
    SELECT dept_id, id
    FROM enriched
    EXCEPT
    SELECT dept_id, id
    FROM employees
    WHERE job_title <> 'Manager'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN remaining_keys k ON s.dept_id = k.dept_id AND s.id = k.id
)
SELECT
    id,
    dept_id,
    salary,
    job_title_filled,
    dept_id_max_salary,
    last_known_email_length,
    cumulative_salary
FROM verified
ORDER BY cumulative_salary
