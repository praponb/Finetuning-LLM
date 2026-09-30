SELECT manager_id, dept_id
FROM employees
INTERSECT
SELECT manager_id, dept_id
FROM employees
WHERE hire_date BETWEEN '2024-09-01' AND '2025-03-01';
