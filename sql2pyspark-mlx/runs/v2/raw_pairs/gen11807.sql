SELECT location, dept_name FROM departments WHERE budget > 3247000 INTERSECT SELECT location, dept_name FROM departments WHERE location NOT IN ('Bangkok', 'Singapore', 'London')
