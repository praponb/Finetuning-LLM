SELECT id, dept_name, budget, NTILE(10) OVER (PARTITION BY dept_name ORDER BY budget, id) quartile FROM departments WHERE dept_name = 'Finance';
