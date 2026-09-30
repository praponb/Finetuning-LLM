SELECT
    CASE WHEN salary >= 134000 THEN 'high' WHEN salary >= 53000 THEN 'mid' ELSE 'low' END AS salary_bucket,
    is_remote,
    manager_id,
    SUM(salary) AS total_salary
FROM employees
WHERE job_title NOT IN ('Designer', 'Intern', 'Analyst') AND hire_date BETWEEN '2023-03-01' AND '2024-08-01'
GROUP BY CASE WHEN salary >= 134000 THEN 'high' WHEN salary >= 53000 THEN 'mid' ELSE 'low' END, is_remote, manager_id
ORDER BY salary_bucket, total_salary DESC
