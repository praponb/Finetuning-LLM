SELECT location, MAX(budget) AS max_budget
FROM departments
WHERE location NOT IN ('Bangkok', 'Singapore', 'Berlin') AND dept_name != 'Finance'
GROUP BY 1
HAVING MAX(budget) < 2470000
ORDER BY location ASC;
