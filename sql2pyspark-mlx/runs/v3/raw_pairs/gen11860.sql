SELECT * FROM (SELECT job_title, level, salary FROM employees WHERE hire_date BETWEEN '2024-06-01' AND '2025-04-06') PIVOT (AVG(salary) FOR level IN ('L4', 'L3', 'L1', 'L5'));
