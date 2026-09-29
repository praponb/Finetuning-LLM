SELECT
    job_title,
    level,
    DATE_FORMAT(hire_date, 'yyyy-MM') AS hire_month,
    AVG(salary) AS avg_salary
FROM employees
WHERE job_title IN ('Manager', 'Intern') AND dept_id = 39 AND full_name IS NOT NULL
GROUP BY job_title, level, DATE_FORMAT(hire_date, 'yyyy-MM')
HAVING AVG(salary) < 101000
ORDER BY avg_salary
