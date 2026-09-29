SELECT location, dept_name FROM departments WHERE budget >= 2990000 INTERSECT SELECT location, dept_name FROM departments WHERE dept_name NOT IN ('Marketing', 'Sales')
