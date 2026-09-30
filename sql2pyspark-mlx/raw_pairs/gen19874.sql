SELECT
    s.id,
    s.dept_name,
    latest.level,
    latest.employee_salary
FROM departments s
LEFT JOIN LATERAL (
        SELECT c.level, c.salary AS employee_salary
        FROM employees c
        WHERE c.dept_id = s.id
        ORDER BY c.hire_date DESC, c.id DESC
        LIMIT 1
    ) latest ON TRUE;
