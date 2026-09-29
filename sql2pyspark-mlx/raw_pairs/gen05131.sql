SELECT location, COUNT(CASE WHEN dept_name = 'Finance' THEN 1 END) AS finance_count
FROM departments
WHERE location = 'Tokyo' AND budget >= 3437000
GROUP BY location
ORDER BY finance_count, location;
