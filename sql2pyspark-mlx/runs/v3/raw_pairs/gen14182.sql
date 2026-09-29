SELECT level, is_remote, min_salary
FROM (
    SELECT level, is_remote, MIN(salary) AS min_salary
    FROM employees
    WHERE job_title NOT IN ('Manager', 'Analyst')
    GROUP BY level, is_remote
) AS t
WHERE min_salary >= 105000;
