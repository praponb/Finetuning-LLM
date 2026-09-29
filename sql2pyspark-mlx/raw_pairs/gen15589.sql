SELECT dept_name, CASE WHEN dept_name = 'HR' THEN 1 ELSE 0 END AS is_hr, LENGTH(dept_name) AS dept_name_length FROM departments
