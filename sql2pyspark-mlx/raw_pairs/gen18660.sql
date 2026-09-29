SELECT CASE WHEN job_title IN ('Engineer', 'Director') THEN 'group_a' ELSE 'other' END AS job_title_group, MAX(salary) - MIN(salary) AS salary_range
FROM employees
GROUP BY job_title_group
ORDER BY salary_range DESC
