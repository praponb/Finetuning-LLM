SELECT job_title, SUM(salary) AS total_salary, SUM(SUM(salary)) OVER (ORDER BY job_title) AS cumulative_salary FROM employees GROUP BY job_title ORDER BY cumulative_salary
