SELECT
    is_remote,
    CASE WHEN level IN ('L4', 'L2') THEN 'tier_1' ELSE 'other' END AS level_group,
    ROUND(AVG(salary), 2) AS avg_salary,
    MAX(salary) - MIN(salary) AS salary_range
FROM employees
GROUP BY ROLLUP(is_remote, CASE WHEN level IN ('L4', 'L2') THEN 'tier_1' ELSE 'other' END);
