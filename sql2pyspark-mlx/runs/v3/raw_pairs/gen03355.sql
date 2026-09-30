SELECT CASE WHEN job_title IN ('Engineer', 'Director') THEN 'group_a' ELSE 'other' END AS job_title_group, SUM(salary) AS total_salary, SUM(SUM(salary)) OVER (ORDER BY CASE WHEN job_title IN ('Engineer', 'Director') THEN 'group_a' ELSE 'other' END) AS cumulative_salary
FROM employees
WHERE salary <= 103000
GROUP BY CASE WHEN job_title IN ('Engineer', 'Director') THEN 'group_a' ELSE 'other' END
ORDER BY job_title_group, cumulative_salary DESC
