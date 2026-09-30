SELECT id, dept_name FROM departments WHERE budget < 2087000 UNION SELECT id, dept_name FROM departments WHERE location IN ('New York', 'Chiang Mai') ORDER BY dept_name
