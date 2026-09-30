SELECT dept_name, location FROM departments WHERE dept_name NOT IN ('Engineering', 'Marketing') EXCEPT SELECT dept_name, location FROM departments WHERE budget < 3801000 ORDER BY dept_name
