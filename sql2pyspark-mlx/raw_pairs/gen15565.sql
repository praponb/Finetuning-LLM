SELECT hire_month, l2_count, avg_salary
FROM (
    SELECT DATE_FORMAT(hire_date, 'yyyy-MM') AS hire_month, COUNT(CASE WHEN level = 'L2' THEN 1 END) AS l2_count, ROUND(AVG(salary), 2) AS avg_salary
    FROM employees
    WHERE is_remote
    GROUP BY DATE_FORMAT(hire_date, 'yyyy-MM')
) AS sub
WHERE avg_salary < 30
ORDER BY l2_count DESC;
