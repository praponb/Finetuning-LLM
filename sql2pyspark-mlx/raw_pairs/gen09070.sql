WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        level,
        DATE_SUB(hire_date, 5) AS hire_date_minus_5d,
        ADD_MONTHS(hire_date, 6) AS hire_date_plus_months
    FROM employees
    WHERE hire_date > DATE '2023-09-04'
      AND job_title IS NOT NULL
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        dept_id,
        job_title,
        salary,
        level,
        hire_date_plus_months
    FROM cleaned
    QUALIFY RANK() OVER (PARTITION BY manager_id ORDER BY hire_date_minus_5d) = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, job_title
    FROM ranked
    EXCEPT
    SELECT id, job_title
    FROM employees
    WHERE hire_date > '2024-07-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    JOIN remaining_keys k ON s.id = k.id AND s.job_title = k.job_title
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        job_title,
        salary,
        level,
        MAX(salary) OVER (PARTITION BY MONTH(hire_date_plus_months), dept_id) AS hire_date_plus_months_month_max_salary,
        ROUND(salary / SUM(salary) OVER (PARTITION BY dept_id, job_title), 4) AS salary_share,
        STDDEV_SAMP(salary) OVER (PARTITION BY level, dept_id) AS level_stddev_samp_salary
    FROM reconciled
    WHERE job_title <> 'Intern'
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        job_title,
        level_stddev_samp_salary,
        CUME_DIST() OVER (PARTITION BY level ORDER BY level_stddev_samp_salary NULLS LAST) AS level_stddev_samp_salary_cume_dist
    FROM metrics
)
-- Aggregate with conditional metrics
SELECT
    job_title,
    ROUND(SUM(level_stddev_samp_salary_cume_dist) / NULLIF(COUNT(CASE WHEN job_title = 'Director' THEN 1 END), 0), 2) AS level_stddev_samp_salary_cume_dist_per_director,
    COUNT_IF(job_title = 'Engineer') AS engineer_events
FROM scored
GROUP BY 1
ORDER BY engineer_events;
