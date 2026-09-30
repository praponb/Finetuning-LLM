SELECT QUARTER(hire_date) AS hire_quarter, SUM(salary) AS total_salary, ROUND(SUM(salary) * 100.0 / SUM(SUM(salary)) OVER (), 2) AS pct_of_total
FROM employees
GROUP BY QUARTER(hire_date)
ORDER BY 2 DESC;
