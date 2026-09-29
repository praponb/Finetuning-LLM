SELECT location, ROUND(STDDEV(budget), 2) AS stddev_budget
FROM departments
GROUP BY location
ORDER BY location ASC;
