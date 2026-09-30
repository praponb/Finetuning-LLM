SELECT dept_name, location, AVG(budget) AS avg_budget
FROM departments
GROUP BY CUBE(dept_name, location)
ORDER BY location DESC, avg_budget;
