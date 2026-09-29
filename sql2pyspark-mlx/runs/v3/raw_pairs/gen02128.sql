SELECT * FROM (SELECT dept_name, COUNT(*) AS total_count FROM departments WHERE budget BETWEEN 2522000 AND 2980000 GROUP BY dept_name) AS agg WHERE total_count >= 5 ORDER BY total_count;
