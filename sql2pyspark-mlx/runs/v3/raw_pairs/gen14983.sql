SELECT level, max_salary
FROM (
    SELECT level, MAX(salary) AS max_salary
    FROM employees
    WHERE job_title IN ('Intern', 'Engineer', 'Director')
    GROUP BY level
) AS x
WHERE max_salary > 106000
ORDER BY max_salary;
