WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY hire_date DESC, id) AS rn
        FROM employees
    ) ranked
    WHERE rn = 1
),
enriched AS (
    -- Attach departments attributes
    SELECT a.*, d.location
    FROM deduped a
    INNER JOIN departments d
        ON a.dept_id = d.id
),
enriched_employees AS (
    -- Window metrics over several different partitions
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
        rn,
        location,
        MIN(salary) OVER (PARTITION BY job_title) AS job_title_min_salary,
        LAG(salary, 1) OVER (PARTITION BY job_title ORDER BY salary, id) AS prev_salary
    FROM enriched
)
SELECT
    id,
    full_name,
    job_title,
    salary,
    hire_date,
    is_remote,
    level,
    job_title_min_salary,
    prev_salary,
    CASE WHEN job_title RLIKE '^(Director|Analyst)$' THEN 'OTHER_ZONE' WHEN job_title RLIKE '^(Engineer|Manager)$' THEN 'PRIORITY' ELSE 'CORE' END AS job_title_zone
FROM enriched_employees
ORDER BY hire_date DESC NULLS LAST
