SELECT level, SUM(CASE WHEN level = 'L3' THEN 1 ELSE 0 END) AS l3_count FROM employees GROUP BY level ORDER BY l3_count DESC
