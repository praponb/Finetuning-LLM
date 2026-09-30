SELECT id, budget, location
FROM departments
WHERE dept_name = 'HR' OR dept_name IN ('Sales', 'Finance', 'Marketing')
