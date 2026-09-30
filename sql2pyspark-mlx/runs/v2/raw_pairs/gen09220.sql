SELECT
    level,
    manager_id,
    is_remote,
    MIN(salary) AS min_salary
FROM employees
WHERE job_title IN ('Designer', 'Intern')
GROUP BY level, manager_id, is_remote
ORDER BY level DESC, manager_id, is_remote
LIMIT 5
