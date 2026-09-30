SELECT e.id, e.hire_date FROM employees e WHERE e.dept_id IN (SELECT id FROM departments WHERE dept_name IN ('Legal', 'HR')) ORDER BY e.hire_date;
