WITH valid AS (
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        ROUND(salary, 1) AS salary_rounded,
        REGEXP_REPLACE(full_name, ' ', '') AS full_name_slug,
        COALESCE(job_title, 'N/A') AS job_title_filled
    FROM employees
    WHERE hire_date >= DATE '2023-02-08'
      AND job_title IS NOT DISTINCT FROM 'Director'
)
SELECT
    a.id,
    a.full_name,
    a.email,
    a.dept_id,
    a.manager_id,
    a.job_title,
    a.job_title_filled,
    d.dept_name
FROM valid a
LEFT JOIN departments d
    ON a.dept_id = d.id;
