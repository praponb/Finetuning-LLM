WITH joined AS (
    SELECT
        src.*,
        e.email AS employee_email,
        e.level AS employee_level
    FROM employees src
    INNER JOIN employees e
        ON src.manager_id = e.id
)
SELECT
    s.id,
    s.full_name,
    s.email,
    s.dept_id,
    s.manager_id,
    s.job_title,
    s.salary,
    s.hire_date,
    s.is_remote,
    (SELECT AVG(t2.salary) FROM employees t2 WHERE t2.manager_id = s.manager_id) AS avg_employee_salary
FROM joined s
ORDER BY job_title NULLS LAST, manager_id;
