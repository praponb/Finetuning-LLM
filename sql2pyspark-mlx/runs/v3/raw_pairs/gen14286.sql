SELECT
    MONTH(hire_date) hire_month,
    YEAR(hire_date) hire_year,
    job_title,
    MAX(salary) max_salary
FROM employees
GROUP BY MONTH(hire_date), YEAR(hire_date), job_title
HAVING max_salary >= 139000
ORDER BY hire_year, job_title, hire_month
LIMIT 1;
