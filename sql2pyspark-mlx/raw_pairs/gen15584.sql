SELECT * FROM (SELECT dept_name, SUM(CASE WHEN location = 'Tokyo' THEN budget ELSE 0 END) AS tokyo_budget FROM departments GROUP BY dept_name) AS t WHERE tokyo_budget >= 48 ORDER BY dept_name;
