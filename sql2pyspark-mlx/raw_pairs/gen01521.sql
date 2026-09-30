WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title, manager_id ORDER BY salary DESC, id) AS row_num
        FROM employees
    ) t
    WHERE row_num = 1
)
SELECT
    s.id,
    s.full_name,
    s.email,
    s.manager_id,
    s.job_title,
    s.hire_date,
    s.is_remote,
    (SELECT MAX(x.salary) FROM employees x WHERE x.manager_id = s.manager_id) AS max_employee_salary
FROM latest s
ORDER BY id;
