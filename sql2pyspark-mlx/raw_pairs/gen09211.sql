SELECT *
FROM (
    SELECT manager_id, ROUND(AVG(salary), 2) AS avg_salary, SUM(CASE WHEN job_title = 'Intern' THEN 1 ELSE 0 END) AS intern_count
    FROM employees
    GROUP BY manager_id
) AS t
WHERE avg_salary < 23;
