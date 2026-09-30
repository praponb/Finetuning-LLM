WITH enriched AS (
    -- Attach departments attributes
    SELECT
        a.id,
        a.email,
        a.dept_id,
        a.salary,
        a.hire_date,
        d.budget,
        d.location,
        d.dept_name
    FROM employees a
    LEFT JOIN departments d
        ON a.dept_id = d.id
)
SELECT
    s.id,
    s.email,
    s.dept_id,
    s.salary,
    s.hire_date,
    s.budget,
    s.location,
    s.dept_name,
    (SELECT MAX(t2.salary) FROM employees t2 WHERE t2.dept_id = s.dept_id) AS max_employee_salary
FROM enriched s
ORDER BY budget;
