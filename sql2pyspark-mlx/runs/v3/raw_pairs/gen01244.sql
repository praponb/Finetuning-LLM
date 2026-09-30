SELECT
    is_remote,
    SUM(CASE WHEN level = 'L2' THEN salary ELSE 0 END) l2_salary,
    ROUND(STDDEV(salary), 2) stddev_salary,
    ROUND(AVG(salary), 2) avg_salary
FROM employees
GROUP BY is_remote
HAVING ROUND(STDDEV(salary), 2) >= 38
