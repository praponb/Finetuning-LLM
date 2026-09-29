WITH cleaned AS (
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
        level,
        CONCAT(email, ' - ', level) email_level,
        COALESCE(level, 'NONE') level_filled
    FROM employees
    WHERE manager_id IS NOT NULL
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        dept_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level_filled,
        SUBSTRING(job_title, 1, 2) job_title_prefix
    FROM cleaned
    WHERE job_title IS NOT NULL
      AND full_name LIKE '%Wong%'
),
matched_keys AS (
    -- Set difference on keys
    SELECT job_title
    FROM base
    EXCEPT
    SELECT job_title
    FROM employees
    WHERE manager_id IS NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN matched_keys k ON s.job_title = k.job_title
),
with_dims AS (
    -- Attach departments attributes
    SELECT src.*, b.budget
    FROM reconciled src
    JOIN departments b
        ON src.dept_id = b.id
)
-- Join a dimension and aggregate
SELECT
    p.dept_name,
    src.job_title,
    COUNT(*) AS num_rows,
    MAX(src.salary) AS max_salary
FROM with_dims src
LEFT JOIN departments p ON src.dept_id = p.id
GROUP BY p.dept_name, src.job_title
ORDER BY max_salary;
