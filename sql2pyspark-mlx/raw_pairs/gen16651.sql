SELECT
    DATE_TRUNC('QUARTER', hire_date) AS quarter_start,
    CASE WHEN salary >= 122000 THEN 'PREMIUM' ELSE 'STANDARD' END AS salary_tier,
    COUNT_IF(job_title = 'Intern') AS intern_events,
    MIN(salary) AS min_salary,
    COUNT(DISTINCT level) AS distinct_levels,
    MAX(salary) FILTER (WHERE level = 'L2') AS l2_max_salary,
    ROUND(AVG(salary), 2) AS avg_salary,
    COUNT_IF(level = 'L5') AS l5_events,
    RANK() OVER (PARTITION BY DATE_TRUNC('QUARTER', hire_date) ORDER BY COUNT_IF(level = 'L5') DESC) AS l5_events_rank
FROM employees
GROUP BY quarter_start, salary_tier
ORDER BY avg_salary NULLS LAST
