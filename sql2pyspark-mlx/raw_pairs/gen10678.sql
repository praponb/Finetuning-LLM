WITH with_dims AS (
    SELECT s.*, p.budget
    FROM employees s
    LEFT JOIN departments p
        ON s.dept_id = p.id
),
valid AS (
    SELECT *, CONCAT(full_name, ' ', level) AS full_name_level
    FROM with_dims
    WHERE manager_id IS NOT NULL
),
cleaned AS (
    SELECT
        id,
        full_name,
        dept_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        budget,
        full_name_level,
        CONCAT_WS('/', full_name, job_title) AS full_name_job_title_key
    FROM valid
    WHERE job_title IN ('Director', 'Intern')
),
remaining_keys AS (
    SELECT id, level
    FROM cleaned
    EXCEPT
    SELECT id, level
    FROM employees
    WHERE manager_id IN (32, 37)
),
verified AS (
    SELECT s.*
    FROM cleaned s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.level = k.level
),
remaining_keys_2 AS (
    SELECT level, id
    FROM verified
    EXCEPT
    SELECT level, id
    FROM employees
    WHERE dept_id IN (1, 9, 18, 23)
),
verified_employees AS (
    SELECT s.*
    FROM verified s
    INNER JOIN remaining_keys_2 k ON s.level = k.level AND s.id = k.id
),
totals AS (
    SELECT
        DATE_TRUNC('QUARTER', hire_date) AS quarter_start,
        COUNT(*) FILTER (WHERE job_title = 'Manager') AS manager_rows,
        ROUND(AVG(salary), 2) AS avg_salary,
        PERCENTILE_APPROX(salary, ARRAY(0.25, 0.5, 0.75)) AS salary_quartiles
    FROM verified_employees
    GROUP BY DATE_TRUNC('QUARTER', hire_date)
)
SELECT
    quarter_start,
    manager_rows,
    avg_salary,
    salary_quartiles
FROM totals;
