WITH with_dims AS (
    SELECT
        s.*,
        b.hire_date AS employee_hire_date,
        b.level AS employee_level,
        b.job_title AS employee_job_title
    FROM employees s
    JOIN employees b
        ON s.manager_id = b.id
    WHERE b.manager_id IN (22, 26, 38)
)
SELECT
    s.id,
    s.full_name,
    s.email,
    s.manager_id,
    s.job_title,
    s.salary,
    s.hire_date,
    s.is_remote,
    s.level,
    (SELECT MAX(t2.salary) FROM employees t2 WHERE t2.manager_id = s.manager_id) AS max_employee_salary
FROM with_dims s;
