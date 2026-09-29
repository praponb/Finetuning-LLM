SELECT location, SUM(CASE WHEN location = 'Bangkok' THEN budget ELSE 0 END) bangkok_budget, SUM(CASE WHEN dept_name = 'Finance' THEN 1 ELSE 0 END) finance_count
FROM departments
WHERE budget > 2013000
GROUP BY location
HAVING COUNT(*) < 5
