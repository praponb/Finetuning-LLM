SELECT job_title, MAX(salary) max_salary, SUM(CASE WHEN level = 'L3' THEN salary ELSE 0 END) l3_salary
FROM employees
WHERE NOT (full_name LIKE '%Lee%' AND email LIKE '%@startup.io' AND salary BETWEEN 97000 AND 118000)
GROUP BY job_title;
