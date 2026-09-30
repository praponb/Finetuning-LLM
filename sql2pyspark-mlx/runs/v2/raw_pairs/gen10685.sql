SELECT dept_name, location FROM departments WHERE budget <= 2880000 UNION ALL SELECT dept_name, location FROM departments WHERE dept_name = 'Legal';
