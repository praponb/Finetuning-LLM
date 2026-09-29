SELECT e.id, e.hire_date, e.email FROM employees e WHERE e.level IN ('L1', 'L5', 'L4') AND e.dept_id IN (SELECT id FROM departments WHERE location IN ('New York', 'Tokyo', 'Chiang Mai'))
