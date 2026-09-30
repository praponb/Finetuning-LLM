SELECT SUM(CASE WHEN location = 'London' THEN 1 ELSE 0 END) AS london_count FROM departments WHERE dept_name IN ('Marketing', 'Legal') ORDER BY london_count DESC;
