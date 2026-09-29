SELECT location, MIN(budget) AS min_budget, COUNT(DISTINCT budget) AS unique_dept_names FROM departments GROUP BY location ORDER BY min_budget DESC;
