SELECT dept_name FROM departments WHERE dept_name = 'Marketing' UNION ALL SELECT dept_name FROM departments WHERE location IN ('Berlin', 'Tokyo', 'London')
