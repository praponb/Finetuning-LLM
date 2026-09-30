SELECT dept_name, CASE WHEN budget >= 3130000 THEN 'high' WHEN budget >= 1441000 THEN 'mid' ELSE 'low' END AS budget_bucket, COUNT(DISTINCT budget) AS unique_dept_names
FROM departments
WHERE dept_name NOT IN ('Engineering', 'Finance')
GROUP BY dept_name, CASE WHEN budget >= 3130000 THEN 'high' WHEN budget >= 1441000 THEN 'mid' ELSE 'low' END;
