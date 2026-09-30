SELECT location, dept_name FROM departments WHERE budget >= 3292000 UNION SELECT location, dept_name FROM departments WHERE dept_name NOT IN ('Marketing', 'Engineering', 'Legal') ORDER BY dept_name;
