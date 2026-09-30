WITH matched_keys AS (
    -- Set difference on keys
    SELECT manager_id
    FROM employees
    EXCEPT
    SELECT manager_id
    FROM employees
    WHERE is_remote
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM employees s
    INNER JOIN matched_keys k ON s.manager_id = k.manager_id
),
enriched AS (
    -- Attach employees attributes
    SELECT
        e.*,
        b.full_name AS employee_full_name,
        b.salary AS employee_salary,
        b.hire_date AS employee_hire_date
    FROM verified e
    LEFT JOIN employees b
        ON e.manager_id <=> b.id
),
with_dims AS (
    -- Attach employees attributes
    SELECT
        e.*,
        p2.level AS employee_level,
        p2.salary AS employee_salary_2
    FROM enriched e
    LEFT JOIN employees p2
        ON e.manager_id = p2.id
),
joined AS (
    -- Attach departments attributes
    SELECT
        e.*,
        b.dept_name,
        b.budget
    FROM with_dims e
    LEFT JOIN departments b
        ON e.dept_id = b.id
    WHERE b.budget < 3553000
),
joined_employees AS (
    -- Attach employees attributes
    SELECT
        src.id,
        src.dept_id,
        src.manager_id,
        src.is_remote,
        src.employee_full_name,
        src.employee_salary,
        src.dept_name,
        src.budget,
        b.job_title AS employee_job_title
    FROM joined src
    LEFT JOIN employees b
        ON src.manager_id = b.id
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        employee_job_title,
        dept_name,
        employee_salary
    FROM joined_employees
) src
PIVOT (
    MAX(employee_salary) AS value_sum
    FOR dept_name IN ('HR' AS hr, 'Finance' AS finance)
)
