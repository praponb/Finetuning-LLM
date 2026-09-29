WITH remaining_keys AS (
    SELECT manager_id, level
    FROM employees
    EXCEPT
    SELECT manager_id, level
    FROM employees
    WHERE job_title IN ('Intern', 'Engineer')
),
kept_rows AS (
    SELECT s.*
    FROM employees s
    INNER JOIN remaining_keys k ON s.manager_id = k.manager_id AND s.level = k.level
)
SELECT
    s.id,
    s.full_name,
    s.manager_id,
    s.job_title,
    s.salary,
    s.hire_date,
    s.is_remote,
    s.level,
    CASE WHEN full_name RLIKE '^(Pim Chaiyaporn|Henry Ford)$' THEN 'PRIORITY' WHEN full_name RLIKE '^(Alice Wong|bob smith)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS full_name_zone,
    (SELECT MAX(x.salary) FROM employees x WHERE x.manager_id = s.manager_id) AS max_employee_salary
FROM kept_rows s
ORDER BY max_employee_salary DESC, hire_date;
