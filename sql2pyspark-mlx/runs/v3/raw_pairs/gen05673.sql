SELECT * FROM (SELECT location, COUNT(DISTINCT budget) AS unique_dept_names, COUNT(budget) AS budget_count FROM departments GROUP BY location) AS x WHERE budget_count > 7
