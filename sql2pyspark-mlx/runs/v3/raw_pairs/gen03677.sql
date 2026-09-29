SELECT location, ROUND(STDDEV(budget), 2) AS stddev_budget
FROM departments
WHERE budget >= 3573000
GROUP BY 1;
