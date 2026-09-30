SELECT level, SUM(CASE WHEN level = 'L2' THEN 1 ELSE 0 END) AS l2_count FROM employees WHERE hire_date > '2024-08-01' AND NOT is_remote AND is_remote GROUP BY level ORDER BY l2_count DESC
