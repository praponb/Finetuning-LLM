SELECT s.*, latest.employee_salary
FROM departments s
JOIN LATERAL (
        SELECT c.salary AS employee_salary
        FROM employees c
        WHERE c.dept_id = s.id
        ORDER BY c.salary DESC, c.id DESC
        LIMIT 1
    ) latest ON TRUE;
