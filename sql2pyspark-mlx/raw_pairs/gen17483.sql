WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(salary AS INT) AS salary_int,
        COALESCE(job_title, 'OTHER') AS resolved_job_title
    FROM employees
    WHERE level IS NOT NULL
      AND level IS NOT DISTINCT FROM 'L1'
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        email,
        manager_id,
        salary,
        hire_date,
        level,
        salary_int,
        DATEDIFF(CURRENT_DATE(), hire_date) / 365 AS years_since_hire_date
    FROM cleaned
    WHERE job_title IS NOT NULL
),
filtered_employees AS (
    -- Keep recent, valid rows
    SELECT *, salary / NULLIF(salary_int, 0) AS salary_per_salary_int
    FROM filtered
    WHERE hire_date >= TO_DATE('2023-02-09')
      AND MONTH(hire_date) IN (7, 8, 10)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        full_name,
        email,
        salary_int,
        salary_per_salary_int,
        CUME_DIST() OVER (PARTITION BY manager_id ORDER BY salary_int DESC NULLS FIRST) AS salary_int_cume_dist,
        LAG(years_since_hire_date, 1) OVER (PARTITION BY YEAR(hire_date), manager_id ORDER BY years_since_hire_date, id) AS prev_years_since_hire_date,
        NTILE(10) OVER (ORDER BY years_since_hire_date, id) AS years_since_hire_date_bucket
    FROM filtered_employees
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM with_windows
    INTERSECT ALL
    SELECT id
    FROM employees
    WHERE hire_date BETWEEN '2023-03-01' AND '2025-01-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN matched_keys k ON s.id = k.id
)
SELECT
    salary_int,
    salary_per_salary_int,
    salary_int_cume_dist,
    prev_years_since_hire_date,
    years_since_hire_date_bucket
FROM kept_rows;
