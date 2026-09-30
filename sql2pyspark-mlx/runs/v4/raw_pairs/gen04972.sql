WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        SUBSTRING(full_name, 1, 3) full_name_prefix,
        DATEDIFF(CURRENT_DATE(), hire_date) days_since_hire_date
    FROM employees
    WHERE hire_date <= '2024-03-14'
),
joined AS (
    -- Attach employees attributes
    SELECT s.*, e.level AS employee_level
    FROM cleaned s
    LEFT JOIN employees e
        ON s.manager_id = e.id
),
scored AS (
    -- Running and moving aggregates
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
        full_name_prefix,
        days_since_hire_date,
        employee_level,
        DENSE_RANK() OVER (PARTITION BY job_title ORDER BY days_since_hire_date NULLS LAST) days_since_hire_date_dense_rank,
        SUM(salary) OVER w_grp running_salary,
        LAST_VALUE(level) IGNORE NULLS OVER w_hist last_known_level
    FROM joined
    WINDOW w_grp AS (PARTITION BY job_title ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_level ORDER BY hire_date, id) = 1
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, job_title
    FROM latest
    INTERSECT ALL
    SELECT id, job_title
    FROM employees
    WHERE hire_date <= '2024-12-31'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    JOIN remaining_keys k ON s.id = k.id AND s.job_title = k.job_title
),
remaining_keys_2 AS (
    -- Set intersection on keys
    SELECT dept_id
    FROM reconciled
    INTERSECT ALL
    SELECT dept_id
    FROM employees
    WHERE salary >= 149000
),
reconciled_employees AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM reconciled s
    JOIN remaining_keys_2 k ON s.dept_id = k.dept_id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        SUBSTRING(full_name, 1, 4) full_name_prefix_2,
        COALESCE(employee_level, 'N/A') employee_level_filled
    FROM reconciled_employees
    WHERE level IS NOT NULL
      AND manager_id = 17
)
SELECT
    s.full_name,
    s.email,
    s.dept_id,
    s.manager_id,
    s.salary,
    s.hire_date,
    s.level,
    s.full_name_prefix,
    s.days_since_hire_date,
    (SELECT SUM(e2.salary) FROM employees e2 WHERE e2.manager_id = s.manager_id) AS sum_employee_salary
FROM filtered s
ORDER BY days_since_hire_date DESC
