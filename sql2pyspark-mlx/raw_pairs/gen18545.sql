WITH base AS (
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), hire_date) AS days_since_hire_date,
        TRIM(level) AS clean_level,
        COALESCE(job_title, 'OTHER') AS job_title_clean
    FROM employees
    WHERE hire_date >= DATE '2023-02-22'
      AND level = 'L3'
),
enriched AS (
    SELECT
        s.*,
        d.level AS employee_level,
        d.email AS employee_email,
        d.full_name AS employee_full_name
    FROM base s
    INNER JOIN employees d
        ON s.manager_id = d.id
),
kept_keys AS (
    SELECT id, level
    FROM enriched
    EXCEPT ALL
    SELECT id, level
    FROM employees
    WHERE email LIKE '%@gmail.com'
),
reconciled AS (
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.id = k.id AND s.level = k.level
),
with_windows AS (
    SELECT
        *,
        ROUND(salary / SUM(salary) OVER (PARTITION BY manager_id), 4) AS salary_share,
        RANK() OVER (ORDER BY salary DESC) AS salary_rank
    FROM reconciled
),
filtered AS (
    SELECT
        *,
        SUBSTRING(full_name, 1, 2) AS full_name_prefix,
        COALESCE(job_title, 'NONE') AS job_title_filled
    FROM with_windows
    WHERE LOWER(employee_email) LIKE '%mma%'
),
joined AS (
    SELECT
        s.full_name,
        s.email,
        s.dept_id,
        s.salary,
        s.hire_date,
        s.is_remote,
        s.level,
        s.days_since_hire_date,
        s.clean_level,
        s.job_title_clean,
        s.employee_email,
        s.full_name_prefix,
        s.job_title_filled,
        d.budget
    FROM filtered s
    LEFT JOIN departments d
        ON s.dept_id = d.id
    WHERE d.budget < 3910000
)
SELECT *
FROM (
    SELECT
        dept_id,
        level,
        days_since_hire_date
    FROM joined
) src
PIVOT (
    MAX(days_since_hire_date)
    FOR level IN ('L1', 'L5')
);
