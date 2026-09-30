SELECT
    job_title,
    YEAR(hire_date) AS hire_year,
    MAX(salary) AS max_salary,
    ROUND(STDDEV(salary), 2) AS stddev_salary
FROM employees
WHERE job_title NOT IN ('Engineer', 'Manager', 'Designer') AND LENGTH(email) > 14
GROUP BY job_title, hire_year
HAVING MAX(salary) < 148000
ORDER BY stddev_salary DESC, hire_year DESC;
