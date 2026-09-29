WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY dept_id ORDER BY salary) AS row_num
    FROM employees
    QUALIFY row_num = 1
),
with_dims AS (
    -- Attach departments attributes
    SELECT e.*, p.location
    FROM ranked e
    INNER JOIN departments p
        ON e.dept_id = p.id
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        INITCAP(TRIM(location)) AS location_title,
        DATE_FORMAT(hire_date, 'yyyy-MM') AS hire_date_label,
        COALESCE(location, 'NONE') AS location_filled
    FROM with_dims
    WHERE salary BETWEEN 69000 AND 76000
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        level,
        row_num,
        location,
        location_title,
        hire_date_label,
        SUBSTRING(full_name, 1, 4) AS full_name_prefix,
        DATEDIFF(CURRENT_DATE(), hire_date) AS days_since_hire_date,
        COALESCE(location, 'N/A') AS resolved_location
    FROM base
    WHERE manager_id IS NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        level,
        row_num,
        location,
        location_title,
        hire_date_label,
        full_name_prefix,
        days_since_hire_date,
        resolved_location,
        AVG(salary) OVER (PARTITION BY job_title, manager_id ORDER BY salary, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS salary_avg_2_prior,
        LEAD(salary, 1) OVER (PARTITION BY dept_id ORDER BY days_since_hire_date, id) AS next_salary,
        NTILE(3) OVER (ORDER BY salary DESC, id) AS salary_bucket
    FROM cleaned
),
ranked_employees AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id, manager_id ORDER BY salary_avg_2_prior DESC NULLS LAST, id) AS latest_rank
    FROM enriched
    QUALIFY latest_rank = 1
)
-- Window metrics over several different partitions
SELECT
    *,
    LEAD(salary, 1, 0) OVER (PARTITION BY dept_id ORDER BY salary_avg_2_prior, id) AS next_salary_2,
    AVG(salary_bucket) OVER (ORDER BY row_num, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS salary_bucket_avg_5_prior,
    FIRST_VALUE(location, TRUE) OVER (PARTITION BY level ORDER BY next_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_location
FROM ranked_employees
