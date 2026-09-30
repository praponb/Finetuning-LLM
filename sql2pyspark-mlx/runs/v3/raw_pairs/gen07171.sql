SELECT manager_id, SUM(CASE WHEN level = 'L2' THEN salary ELSE 0 END) AS l2_salary, SUM(CASE WHEN level = 'L5' THEN salary ELSE 0 END) AS l5_salary FROM employees GROUP BY manager_id;
