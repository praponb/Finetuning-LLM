SELECT
    d.id,
    d.location,
    d.dept_name,
    d.budget
FROM departments d
WHERE d.budget <= 3852000 AND d.id IN (SELECT dept_id FROM employees WHERE MONTH(hire_date) IN (2, 4, 10));
