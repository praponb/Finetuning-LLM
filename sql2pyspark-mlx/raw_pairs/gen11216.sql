SELECT dept_name, SUM(CASE WHEN location = 'Chiang Mai' THEN 1 ELSE 0 END) AS chiang_mai_count FROM departments WHERE budget <= 1895000 GROUP BY dept_name ORDER BY chiang_mai_count, 1 LIMIT 5
