SELECT id, location, dept_name, CASE WHEN dept_name = 'Legal' THEN 1 ELSE 0 END AS is_legal FROM departments WHERE budget >= 1711000 ORDER BY 1 DESC, location DESC
