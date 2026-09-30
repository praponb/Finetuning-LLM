SELECT id, location, budget, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget, id) AS rn FROM departments WHERE dept_name = 'Legal' ORDER BY location, id DESC;
