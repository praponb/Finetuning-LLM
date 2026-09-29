WITH valid AS (
    SELECT *, INITCAP(TRIM(location)) AS location_title
    FROM departments
    WHERE dept_name IS NOT NULL
      AND location IS DISTINCT FROM 'Singapore'
)
SELECT
    s.id,
    s.location_title,
    latest.employee_job_title,
    latest.employee_salary
FROM valid s
LEFT JOIN LATERAL (
        SELECT c.job_title AS employee_job_title, c.salary AS employee_salary
        FROM employees c
        WHERE c.dept_id = s.id
        ORDER BY c.salary DESC, c.id DESC
        LIMIT 1
    ) latest ON TRUE;
