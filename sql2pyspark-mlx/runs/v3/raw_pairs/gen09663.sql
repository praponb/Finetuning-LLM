SELECT is_remote, SUM(CASE WHEN level = 'L3' THEN salary ELSE 0 END) AS l3_salary FROM employees GROUP BY is_remote ORDER BY l3_salary;
