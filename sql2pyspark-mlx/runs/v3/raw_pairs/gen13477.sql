SELECT YEAR(hire_date) hire_year, is_remote, MIN(salary) min_salary, SUM(salary) total_salary FROM employees GROUP BY YEAR(hire_date), is_remote HAVING min_salary > 137000 ORDER BY min_salary
