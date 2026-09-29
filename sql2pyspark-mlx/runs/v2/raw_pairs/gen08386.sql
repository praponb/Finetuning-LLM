SELECT YEAR(hire_date) hire_year, SUM(salary) total_salary, DENSE_RANK() OVER (ORDER BY SUM(salary) DESC) AS rnk
FROM employees
GROUP BY YEAR(hire_date)
