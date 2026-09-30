WITH matched_keys AS (
    SELECT dept_id, level
    FROM employees
    EXCEPT ALL
    SELECT dept_id, level
    FROM employees
    WHERE manager_id IN (12, 24, 26)
),
kept_rows AS (
    SELECT s.*
    FROM employees s
    INNER JOIN matched_keys k ON s.dept_id = k.dept_id AND s.level = k.level
)
SELECT
    s.id,
    s.full_name,
    s.email,
    s.dept_id,
    s.manager_id,
    s.salary,
    s.is_remote,
    CASE WHEN full_name RLIKE '^(Alice Wong|Olivia Garcia)$' THEN 'CORE' ELSE 'PRIORITY' END AS full_name_zone,
    (SELECT AVG(t2.salary) FROM employees t2 WHERE t2.dept_id = s.dept_id) AS avg_employee_salary
FROM kept_rows s;
