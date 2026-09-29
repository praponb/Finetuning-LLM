SELECT e.id, e.full_name, e.level, e.email FROM employees e WHERE e.dept_id IN (SELECT id FROM departments WHERE location IN ('Berlin', 'Chiang Mai')) ORDER BY e.full_name
