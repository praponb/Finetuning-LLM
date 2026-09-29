SELECT dept_name, ROUND((100 * SUM(CASE WHEN location = 'New York' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_new_york FROM departments GROUP BY 1 ORDER BY dept_name;
