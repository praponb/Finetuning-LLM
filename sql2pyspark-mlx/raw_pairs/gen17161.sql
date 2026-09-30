WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LOWER(full_name) AS full_name_lower,
        COALESCE(job_title, 'OTHER') AS resolved_job_title
    FROM employees
    WHERE job_title IS NOT DISTINCT FROM 'Engineer'
      AND job_title <> 'Intern'
),
matched_keys AS (
    -- Set intersection on keys
    SELECT manager_id, level
    FROM filtered
    INTERSECT
    SELECT manager_id, level
    FROM employees
    WHERE job_title = 'Intern'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    JOIN matched_keys k ON s.manager_id = k.manager_id AND s.level = k.level
),
joined AS (
    -- Attach departments attributes
    SELECT e.*, d.budget
    FROM verified e
    LEFT JOIN departments d
        ON e.dept_id = d.id
    WHERE d.budget < 3629000
),
filtered_employees AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        hire_date,
        is_remote,
        level,
        full_name_lower,
        resolved_job_title,
        budget,
        COALESCE(job_title, 'unassigned') AS job_title_filled,
        COALESCE(job_title, 'NONE') AS job_title_filled_2
    FROM joined
    WHERE level = 'L4'
)
SELECT
    id,
    full_name,
    manager_id,
    hire_date,
    is_remote,
    full_name_lower,
    resolved_job_title,
    budget,
    job_title_filled_2
FROM filtered_employees
ORDER BY budget DESC;
