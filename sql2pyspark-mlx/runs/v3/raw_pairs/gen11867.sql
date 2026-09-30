SELECT location, dept_name, SUM(CASE WHEN dept_name = 'Marketing' THEN budget ELSE 0 END) AS marketing_budget
FROM departments
WHERE budget BETWEEN 3138000 AND 3669000
GROUP BY ROLLUP(location, dept_name)
ORDER BY dept_name DESC;
