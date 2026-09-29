SELECT
    job_title,
    ROUND(AVG(salary), 2) AS avg_salary,
    MIN(hire_date) AS first_hire_date,
    MIN(salary) AS min_salary
FROM employees
GROUP BY job_title;
