SELECT id, budget, dept_name, FIRST_VALUE(dept_name) OVER (ORDER BY budget DESC, id) AS first_dept_name FROM departments WHERE location IN ('Tokyo', 'Berlin');
