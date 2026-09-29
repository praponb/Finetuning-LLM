SELECT * FROM (SELECT dept_name, MAX(budget) - MIN(budget) AS budget_range FROM departments WHERE budget < 3082000 GROUP BY dept_name) AS x WHERE budget_range < 29 ORDER BY budget_range DESC
