SELECT budget, dept_name, LENGTH(dept_name) AS dept_name_length FROM departments WHERE location IS NOT NULL ORDER BY 2
