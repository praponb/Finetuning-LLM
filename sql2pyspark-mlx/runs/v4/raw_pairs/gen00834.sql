WITH filtered AS (
    SELECT *, DATE_FORMAT(hire_date, 'yyyy-MM') AS hire_date_label
    FROM employees
    WHERE job_title IS NOT NULL
),
ranked AS (
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary DESC, id ASC) = 1
),
enriched AS (
    SELECT
        s.*,
        d.salary AS employee_salary,
        d.job_title AS employee_job_title,
        d.level AS employee_level
    FROM ranked s
    JOIN employees d
        ON s.manager_id = d.id
),
metrics AS (
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
        hire_date_label,
        employee_salary,
        employee_job_title,
        employee_level,
        ROW_NUMBER() OVER (ORDER BY salary DESC, id ASC) AS row_seq,
        LAG(employee_salary, 1) OVER (PARTITION BY employee_job_title ORDER BY employee_salary ASC, id ASC) AS prev_employee_salary
    FROM enriched
),
valid AS (
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
        hire_date_label,
        employee_salary,
        employee_job_title,
        employee_level,
        prev_employee_salary,
        CASE WHEN salary >= 138000 THEN 'High' WHEN salary >= 70000 THEN 'Medium' ELSE 'Low' END AS salary_band,
        COALESCE(job_title, 'UNKNOWN') AS job_title_filled
    FROM metrics
    WHERE manager_id IS NOT NULL
      AND job_title NOT IN ('Intern', 'Designer', 'Analyst')
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id, job_title ORDER BY employee_salary DESC, id ASC) AS rank_in_group
        FROM valid
    ) x
    WHERE rank_in_group = 1
),
grouped AS (
    SELECT
        employee_job_title,
        COUNT(CASE WHEN job_title = 'Analyst' THEN 1 END) AS analyst_count,
        COUNT(*) AS cnt,
        COUNT_IF(employee_job_title = 'Engineer') AS engineer_events
    FROM deduped
    GROUP BY employee_job_title
    HAVING analyst_count < 1
)
SELECT
    employee_job_title,
    analyst_count,
    cnt
FROM grouped
