WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(level) AS clean_level,
        COALESCE(level, 'NONE') AS level_filled
    FROM employees
    WHERE dept_id IN (26, 39, 40)
      AND hire_date >= TO_DATE('2023-08-01')
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        full_name,
        email,
        manager_id,
        job_title,
        hire_date,
        level_filled,
        SUM(salary) OVER (PARTITION BY manager_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary,
        CUME_DIST() OVER (ORDER BY salary DESC) AS salary_cume_dist,
        SUM(salary) OVER (PARTITION BY job_title ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary_2,
        NTILE(3) OVER w_ordered AS salary_bucket
    FROM cleaned
    WHERE manager_id = 13
    WINDOW w_ordered AS (PARTITION BY job_title ORDER BY salary, id)
),
cleaned_employees AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(running_salary_2, 0) AS running_salary_2_rounded,
        COALESCE(running_salary_2, 0) AS running_salary_2_filled
    FROM enriched
    WHERE hire_date > TO_DATE('2023-05-14')
      AND manager_id IS NOT NULL
),
matched_keys AS (
    -- Set difference on keys
    SELECT job_title, manager_id
    FROM cleaned_employees
    EXCEPT ALL
    SELECT job_title, manager_id
    FROM employees
    WHERE manager_id IN (18, 25, 30)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned_employees s
    INNER JOIN matched_keys k ON s.job_title = k.job_title AND s.manager_id = k.manager_id
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SUBSTRING(full_name, 1, 2) AS full_name_prefix,
        DATE_SUB(hire_date, 47) AS hire_date_minus_47d
    FROM verified
    WHERE hire_date >= DATE '2023-07-30'
)
-- Join a dimension and aggregate
SELECT
    p.level,
    e.job_title AS employee_job_title,
    COUNT(*) AS cnt,
    ROUND(SUM(e.running_salary_2_rounded), 2) AS total_running_salary_2_rounded,
    MIN(e.salary_cume_dist) AS min_salary_cume_dist,
    GROUPING_ID() AS gid
FROM filtered e
LEFT JOIN employees p ON e.manager_id = p.id
GROUP BY GROUPING SETS ((p.level, e.job_title), (p.level));
