SELECT location, dept_name, MIN(budget) AS min_budget FROM departments GROUP BY CUBE(location, dept_name);
